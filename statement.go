package mysqldriver

import (
	"database/sql/driver"
	"io"
)

type mysqlStmt struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
}

func (stmt *mysqlStmt) Close() error {
	if stmt.mc == nil || stmt.mc.closed.Load() {
		return nil
	}
	err := stmt.mc.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.mc = nil
	return err
}
func (stmt *mysqlStmt) NumInput() int {
	return stmt.paramCount
}
func (stmt *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc.closed.Load() {
		return nil, driver.ErrBadConn
	}

	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc
	handleOk := stmt.mc.clearResult()

	resLen, err := handleOk.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	if resLen > 0 {
		// columns
		if err := mc.readUntilEOF(); err != nil {
			return nil, err
		}
		// rows
		if err := mc.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if err := handleOk.discardResults(); err != nil {
		return nil, err
	}

	copied := mc.result
	return &copied, nil
}

func (stmt *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query(args)
}
func (stmt *mysqlStmt) query(args []driver.Value) (*binaryRows, error) {
	if stmt.mc.closed.Load() {
		return nil, driver.ErrBadConn
	}

	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc
	handleOk := stmt.mc.clearResult()
	resLen, err := handleOk.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := new(binaryRows)

	if resLen > 0 {
		rows.mc = mc
		rows.rs.columns, err = mc.readColumns(resLen)
	} else {
		rows.rs.done = true

		switch err := rows.NextResultSet(); err {
		case nil, io.EOF:
			return rows, nil
		default:
			return nil, err
		}
	}

	return rows, err
}

type converter struct{}

func (c converter) ConvertValue(v any) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}
	panic("not implemented") // TODO: implement
}
