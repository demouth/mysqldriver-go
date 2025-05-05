package mysqldriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
)

type mysqlConn struct {
	buf              buffer
	netConn          net.Conn
	result           mysqlResult
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

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) (err error) {
	handleOk := mc.clearResult()
	if err = mc.writeCommandPacket(comPing); err != nil {
		return mc.markBadConn(err)
	}
	return handleOk.readResultOK()
}

func (mc *mysqlConn) resetSequence() {
	mc.sequence = 0
}

func (mc *mysqlConn) markBadConn(err error) error {
	return errors.New("driver: bad connection")
}
