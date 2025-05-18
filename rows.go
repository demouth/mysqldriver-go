package mysqldriver

import (
	"database/sql/driver"
	"io"
)

type resultSet struct {
	columns     []mysqlField
	columnNames []string
	done        bool
}

type mysqlRows struct {
	mc     *mysqlConn
	rs     resultSet
	finish func()
}
type binaryRows struct {
	mysqlRows
}
type textRows struct {
	mysqlRows
}

func (rows *mysqlRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}
	columns := make([]string, len(rows.rs.columns))

	for i := range columns {
		columns[i] = rows.rs.columns[i].name
	}

	rows.rs.columnNames = columns
	return columns
}
func (rows *mysqlRows) Close() error {
	return nil // TODO: implement close
}

func (rows *mysqlRows) HasNextResultSet() (b bool) {
	if rows.mc == nil {
		return false
	}
	return rows.mc.status&statusMoreResultsExists != 0
}

func (rows *mysqlRows) nextResultSet() (int, error) {
	if rows.mc == nil {
		return 0, io.EOF
	}
	if err := rows.mc.error(); err != nil {
		return 0, err
	}

	if !rows.rs.done {
		if err := rows.mc.readUntilEOF(); err != nil {
			return 0, err
		}
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		rows.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}

	resLen, err := rows.mc.resultUnchanged().readResultSetHeaderPacket()
	if err != nil {
		rows.rs.done = true
		rows.mc.status = rows.mc.status & (^statusMoreResultsExists)
	}
	return resLen, err
}

func (rows *mysqlRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}
		if resLen > 0 {
			return resLen, nil
		}
		rows.rs.done = true
	}
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}
		return rows.readRow(dest)
	}
	return io.EOF
}
func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}
		return rows.readRow(dest)
	}
	return io.EOF
}
