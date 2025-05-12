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

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}
		return rows.readRow(dest)
	}
	return io.EOF
}
