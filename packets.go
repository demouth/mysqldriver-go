package mysqldriver

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		data, err := readNext(4, mc.readWithTimeout)
		if err != nil {
			mc.close()
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			return nil, err
		}

		// packet length
		pkLen := getUint24(data[:3])
		seq := data[3]

		// packet sequence
		if seq != mc.sequence {
			return nil, errors.New("commands out of sync.")
		}
		mc.sequence++

		// read packet body
		data, err = readNext(pkLen, mc.readWithTimeout)
		if err != nil {
			mc.close()
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			return nil, err
		}

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
		mc.cleanup()
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
			mc.cleanup()
			if cerr := mc.canceled.Value(); cerr != nil {
				return cerr
			}
			return err
		}
		if n != 4+size {
			mc.cleanup()
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
		mc.cleanup()
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

func (mc *okHandler) readResultSetHeaderPacket() (int, error) {
	mc.result.affectedRows = append(mc.result.affectedRows, 0)
	mc.result.insertIds = append(mc.result.insertIds, 0)

	data, err := mc.conn().readPacket()
	if err != nil {
		return 0, err
	}

	switch data[0] {
	case iOK:
		return 0, mc.handleOkPacket(data)
	}

	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
	num, _, _ := readLengthEncodedInteger(data)
	return int(num), nil
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

func (mc *mysqlConn) writeCommandPacketStr(command byte, arg string) error {
	mc.resetSequence()

	pktLen := 1 + len(arg)
	data, err := mc.buf.takeBuffer(pktLen + 4)
	if err != nil {
		return err
	}
	data[4] = command
	copy(data[5:], arg)
	err = mc.writePacket(data)
	mc.syncSequence()
	return err
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html#sect_protocol_com_query_response_text_resultset_column_definition_41
func (mc *mysqlConn) readColumns(count int) ([]mysqlField, error) {
	columns := make([]mysqlField, count)
	for i := 0; ; i++ {
		data, err := mc.readPacket()
		if err != nil {
			return nil, err
		}

		if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
			if i == count {
				return columns, nil
			}
			return nil, fmt.Errorf("column count mismatch n:%d len:%d", count, len(columns))
		}

		// Catalog
		pos, err := skipLengthEncodedString(data)
		if err != nil {
			return nil, err
		}

		// Database
		n, err := skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Table
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Original table
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Name
		name, _, n, err := readLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		columns[i].name = string(name)
		pos += n

		// Original name
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// filter
		pos++

		// character set
		columns[i].charSet = data[pos]
		pos += 2

		// column length
		columns[i].length = binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		// column type
		columns[i].fieldType = fieldType(data[pos])
		pos++

		// flags
		columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2

		// decimals
		columns[i].decimals = data[pos]
		pos += 1
	}
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
func (rows *textRows) readRow(dest []driver.Value) error {
	mc := rows.mc

	if rows.rs.done {
		return io.EOF
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == iEOF && len(data) == 5 {
		rows.mc.status = readStatus(data[3:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.mc = nil
		}
		return io.EOF
	}
	if data[0] == iERR {
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	var (
		n      int
		isNull bool
		pos    int = 0
	)

	for i := range dest {

		var buf []byte
		buf, isNull, n, err = readLengthEncodedString(data[pos:])
		pos += n
		if err != nil {
			return err
		}

		if isNull {
			dest[i] = nil
			continue
		}

		switch rows.rs.columns[i].fieldType {
		default:
			dest[i] = buf
		}

	}
	return nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
func (mc *mysqlConn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return errors.New("malformed packet")
	}
	errno := binary.LittleEndian.Uint16(data[1:3])
	me := &MySQLError{
		Number: errno,
	}
	pos := 3
	if data[3] == 0x23 {
		copy(me.SQLState[:], data[4:4+5])
		pos = 9
	}
	me.Message = string(data[pos:])
	return me
}
