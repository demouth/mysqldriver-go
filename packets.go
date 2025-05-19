package mysqldriver

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
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

func (mc *okHandler) discardResults() error {
	for mc.status&statusMoreResultsExists != 0 {
		resLen, err := mc.readResultSetHeaderPacket()
		if err != nil {
			return err
		}
		if resLen > 0 {
			// columns
			if err := mc.conn().readUntilEOF(); err != nil {
				return err
			}
			// rows
			if err := mc.conn().readUntilEOF(); err != nil {
				return err
			}
		}
	}
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

func (mc *mysqlConn) writeCommandPacketUint32(command byte, arg uint32) error {
	mc.resetSequence()

	data, err := mc.buf.takeSmallBuffer(4 + 1 + 4)
	if err != nil {
		return err
	}

	data[4] = command

	binary.LittleEndian.PutUint32(data[5:], arg)

	return mc.writePacket(data)
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

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
func (rows *binaryRows) readRow(dest []driver.Value) error {
	data, err := rows.mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] != iOK {
		if data[0] == iEOF && len(data) == 5 {
			rows.mc.status = readStatus(data[3:])
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				rows.mc = nil
			}
			return io.EOF
		}
		mc := rows.mc
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	pos := 1 + (len(dest)+7+2)>>3
	nullMask := data[1:pos]

	for i := range dest {
		// check if the value is NULL
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		switch rows.rs.columns[i].fieldType {
		// Numeric Types
		case fieldTypeTiny:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = uint64(int8(data[pos]))
			}
			pos++
			continue
		case fieldTypeShort, fieldTypeYear:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = uint64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue
		case fieldTypeInt24, fieldTypeLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue
		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
			fieldTypeVector:
			var isNull bool
			var n int
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err
		default:
			return fmt.Errorf("unsupported field type: %d", rows.rs.columns[i].fieldType)
		}
	}
	return nil
}

func (mc *mysqlConn) readUntilEOF() error {
	for {
		data, err := mc.readPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case iERR:
			return mc.handleErrorPacket(data)
		case iEOF:
			if len(data) == 5 {
				mc.status = readStatus(data[3:])
			}
			return nil
		}
	}
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

// Prepared Statements

func (stmt *mysqlStmt) readPrepareResultPacket() (uint16, error) {
	data, err := stmt.mc.readPacket()
	if err == nil {
		// status [1 byte]
		if data[0] != iOK {
			return 0, stmt.mc.handleErrorPacket(data)
		}
		// statement_id [4 bytes]
		stmt.id = binary.LittleEndian.Uint32(data[1:5])

		// num_columns [2 bytes]
		columnCount := binary.LittleEndian.Uint16(data[5:7])

		// num_params [2 bytes]
		stmt.paramCount = int(binary.LittleEndian.Uint16(data[7:9]))

		return columnCount, nil
	}
	return 0, err
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
func (stmt *mysqlStmt) writeExecutePacket(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPkLen = 4 + 1 + 4 + 1 + 4
	mc := stmt.mc

	longDataSize := mc.maxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	mc.resetSequence()

	var data []byte
	var err error

	if len(args) == 0 {
		data, err = mc.buf.takeBuffer(minPkLen)
	} else {
		data, err = mc.buf.takeCompleteBuffer()
	}

	if err != nil {
		return err
	}

	// status [1 byte]
	data[4] = comStmtExecute

	// statement_id [4 bytes]
	binary.LittleEndian.PutUint32(data[5:], stmt.id)

	// flags CURSOR_TYPE_NO_CURSOR [1 byte]
	data[9] = 0x00

	// iteration_count [4 bytes]
	binary.LittleEndian.PutUint32(data[10:], 1)

	if len(args) > 0 {
		pos := minPkLen
		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= cap(data) {
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := range nullMask {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		// new_params_bind_flag [1 byte]
		data[pos] = 0x01
		pos++

		// parameter type [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// paramter values [n bytes]
		// NOTE: https://go.dev/play/p/6i4gmo8TkWf
		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+1] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00
				continue
			}

			if v, ok := arg.(json.RawMessage); ok {
				arg = []byte(v)
			}

			switch v := arg.(type) {
			case string:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00
				if len(v) < longDataSize {
					paramValues = appendLengthEncodedInteger(paramValues, uint64(len(v)))
					paramValues = append(paramValues, v...)
				} else {
					if err := stmt.writeCommandLongData(i, []byte(v)); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}

		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
			mc.buf.store(data)
		}

		pos += len(paramValues)
		data = data[:pos]
	}

	err = mc.writePacket(data)
	mc.syncSequence()
	return err
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
func (stmt *mysqlStmt) writeCommandLongData(paramID int, arg []byte) error {
	maxLen := stmt.mc.maxAllowedPacket - 1
	pktLen := maxLen

	// 4 bytes for the packet header
	// 1 byte for the command
	// 4 bytes for the statement ID
	// 2 bytes for the parameter ID
	const dataOffset = 1 + 4 + 2

	data := make([]byte, 4+dataOffset+len(arg))

	copy(data[4+dataOffset:], arg)

	for argLen := len(arg); argLen > 0; argLen -= pktLen - dataOffset {
		if dataOffset+argLen < maxLen {
			pktLen = dataOffset + argLen
		}

		stmt.mc.resetSequence()

		// 1 byte for the command
		data[4] = comStmtSendLongData

		// 4 bytes for the statement ID
		binary.LittleEndian.PutUint32(data[5:], stmt.id)

		// 2 bytes for the parameter ID
		binary.LittleEndian.PutUint16(data[9:], uint16(paramID))

		err := stmt.mc.writePacket(data[:4+pktLen])
		if err != nil {
			data = data[pktLen-dataOffset:]
			continue
		}
		return err
	}

	stmt.mc.resetSequence()
	return nil
}
