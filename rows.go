package mysqldriver

import "database/sql/driver"

type resultSet struct {
	columns []mysqlField
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
	return nil
}
func (rows *mysqlRows) Close() error {
	return nil
}

func (rows *textRows) Next(dest []driver.Value) error {
	return nil
}
