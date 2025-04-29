package mysqldriver

import (
	"database/sql/driver"
	"net"
)

type mysqlConn struct {
	buf              buffer
	netConn          net.Conn
	cfg              *Config
	connector        *connector
	maxAllowedPacket int
	status           statusFlag
	sequence         uint8
}

func (mc *mysqlConn) writeWithTimeout(b []byte) (int, error) {
	return mc.netConn.Write(b)
}
func (mc *mysqlConn) readWithTimeout(b []byte) (int, error) {
	return mc.netConn.Read(b)
}
func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}
func (mc *mysqlConn) Begin() (driver.Tx, error) {
	return nil, nil
}
func (mc *mysqlConn) Close() (err error) {
	return nil
}
