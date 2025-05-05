package mysqldriver

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func (mc *mysqlConn) readHandshakePacket() (data []byte, plugin string, err error) {
	data, err = mc.readPacket()
	if err != nil {
		return
	}

	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	// first part of password cipher
	authData := data[pos : pos+8]

	// filter
	pos += 8 + 1

	// capability flags
	pos += 2

	if len(data) > pos {

		// character set
		pos += 1
		// status flags
		pos += 2
		// capability flags
		pos += 2
		// auth-plugin-data
		pos += 1
		// reserved
		pos += 10

		// second part of password cipher
		authData = append(authData, data[pos:pos+12]...)
		pos += 13

		if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
			plugin = string(data[pos : pos+end])
		} else {
			plugin = string(data[pos:])
		}

		var b [20]byte
		copy(b[:], authData)
		return b[:], plugin, nil
	}
	var b [8]byte
	copy(b[:], authData)
	return b[:], plugin, nil
}

// MySQL client/server protocol documentations.
// https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html
func (mc *mysqlConn) readPacket() ([]byte, error) {
	var prevData []byte
	readNext := mc.buf.readNext
	for {
		// packet header
		data, _ := readNext(4, mc.readWithTimeout)

		// packet length
		pkLen := getUint24(data[:3])
		seq := data[3]

		// packet sequence
		if seq != mc.sequence {
			return nil, errors.New("commands out of sync.")
		}
		mc.sequence++

		// read packet body
		data, _ = readNext(pkLen, mc.readWithTimeout)

		// check if the packet is complete
		if pkLen < maxPacketSize {
			if prevData != nil {
				data = append(prevData, data...)
			}
			return data, nil
		}

		// read next packet
		prevData = append(prevData, data...)
	}
}

func (mc *mysqlConn) readAuthResult() ([]byte, string, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, "", err
	}

	switch data[0] {
	case iAuthMoreData:
		return data[1:], "", err
	default:
		return nil, "", errors.New("malformed packet")
	}
}

// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (mc *mysqlConn) writeHandshakeResponsePacket(authResp []byte, plugin string) error {
	clientFlags := clientProtocol41 |
		clientSecureConn |
		clientLongPassword |
		clientTransactions |
		clientLocalFiles |
		clientPluginAuth |
		clientMultiResults |
		clientConnectAttrs |
		clientLongFlag

	sendConnectAttrs := true

	var authRespLEIBuf [9]byte
	authRespLen := len(authResp)
	authRespLEI := appendLengthEncodedInteger(authRespLEIBuf[:0], uint64(authRespLen))

	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1 + len(authRespLEI) + len(authResp) + 21 + 1

	if n := len(mc.cfg.DBName); n > 0 {
		clientFlags |= clientConnectWithDB
		pktLen += n + 1
	}

	var connAttrsLEI []byte
	if sendConnectAttrs {
		var connAttrsLEIBuf [9]byte
		connAttrsLen := len(mc.connector.encodedAttributes)
		connAttrsLEI = appendLengthEncodedInteger(connAttrsLEIBuf[:0], uint64(connAttrsLen))
		pktLen += len(connAttrsLEI) + len(mc.connector.encodedAttributes)
	}

	data, err := mc.buf.takeBuffer(pktLen + 4)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(data[4:], uint32(clientFlags))

	binary.LittleEndian.PutUint32(data[8:], 0)

	data[12] = defaultCollationID

	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	if len(mc.cfg.User) > 0 {
		pos += copy(data[pos:], []byte(mc.cfg.User))
	}
	data[pos] = 0x00
	pos++

	pos += copy(data[pos:], authRespLEI)
	pos += copy(data[pos:], authResp)

	if len(mc.cfg.DBName) > 0 {
		pos += copy(data[pos:], mc.cfg.DBName)
		data[pos] = 0x00
		pos++
	}

	pos += copy(data[pos:], plugin)
	data[pos] = 0x00
	pos++

	if sendConnectAttrs {
		pos += copy(data[pos:], connAttrsLEI)
		pos += copy(data[pos:], []byte(mc.connector.encodedAttributes))
	}

	return mc.writePacket(data[:pos])
}

func (mc *mysqlConn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	writeFunc := mc.writeWithTimeout

	for {
		size := min(maxPacketSize, pktLen)
		putUint24(data[:3], size)
		data[3] = mc.sequence

		n, err := writeFunc(data[:4+size])
		if err != nil {
			return err
		}
		if n != 4+size {
			return errors.New("short write")
		}

		mc.sequence++
		if size != maxPacketSize {
			return nil
		}

		pktLen -= size
		data = data[size:]

	}
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (mc *mysqlConn) writeAuthSwitchPacket(authData []byte) error {
	pktLen := 4 + len(authData)
	data, err := mc.buf.takeBuffer(pktLen)
	if err != nil {
		return err
	}
	copy(data[4:], authData)
	return mc.writePacket(data)
}

func (mc *mysqlConn) resultUnchanged() *okHandler {
	return (*okHandler)(mc)
}

type okHandler mysqlConn

func (mc *okHandler) conn() *mysqlConn {
	return (*mysqlConn)(mc)
}

func (mc *okHandler) readResultOK() error {
	data, err := mc.conn().readPacket()
	if err != nil {
		return err
	}
	if data[0] == iOK {
		return mc.handleOkPacket(data)
	}
	return errors.New("malformed packet")
}

// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet
func (mc *okHandler) handleOkPacket(data []byte) error {
	var n, m int
	var affectedRows, insertId uint64

	// Affected rows [Length Coded Binary]
	affectedRows, _, n = readLengthEncodedInteger(data[1:])
	// Insert id [Length Coded Binary]
	insertId, _, m = readLengthEncodedInteger(data[1+n:])

	// TODO
	_ = affectedRows
	_ = insertId

	mc.status = readStatus(data[1+n+m : 1+n+m+2])

	return nil
}

func readStatus(b []byte) statusFlag {
	return statusFlag(b[0]) | statusFlag(b[1])<<8
}

func (mc *mysqlConn) clearResult() *okHandler {
	mc.result = mysqlResult{}
	return (*okHandler)(mc)
}

func (mc *mysqlConn) writeCommandPacket(command byte) error {
	mc.resetSequence()
	data, err := mc.buf.takeSmallBuffer(4 + 1)
	if err != nil {
		return err
	}
	data[4] = command
	return mc.writePacket(data)
}
