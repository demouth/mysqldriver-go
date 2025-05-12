package mysqldriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"sync/atomic"
)

type mysqlConn struct {
	buf              buffer
	netConn          net.Conn
	rawConn          net.Conn
	result           mysqlResult
	cfg              *Config
	connector        *connector
	maxAllowedPacket int
	status           statusFlag
	sequence         uint8

	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError
	closed   atomic.Bool
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

func (mc *mysqlConn) close() {
	// TODO: implement close
	mc.cleanup()
}

func (mc *mysqlConn) cleanup() {
	if mc.closed.Swap(true) {
		return
	}

	conn := mc.rawConn
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		// TODO: handle error
	}
}

func (mc *mysqlConn) error() error {
	if mc.closed.Load() {
		if err := mc.canceled.Value(); err != nil {
			return err
		}
		return errors.New("invalid connection")
	}
	return nil
}

func (mc *mysqlConn) watchCancel(ctx context.Context) error {
	if mc.watching {
		mc.cleanup()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		return nil
	}
	if mc.watcher == nil {
		return nil
	}

	mc.watching = true
	mc.watcher <- ctx
	return nil
}

func (mc *mysqlConn) startWatcher() {
	watcher := make(chan context.Context, 1)
	mc.watcher = watcher
	finished := make(chan struct{})
	mc.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-mc.closech:
				return
			}

			select {
			case <-ctx.Done():
				mc.cancel(ctx.Err())
			case <-finished:
			case <-mc.closech:
				return
			}
		}
	}()
}

func (mc *mysqlConn) finish() {
	if !mc.watching || mc.finished == nil {
		return
	}
	select {
	case mc.finished <- struct{}{}:
		mc.watching = false
	case <-mc.closech:
	}
}

func (mc *mysqlConn) cancel(err error) {
	mc.canceled.Set(err)
	mc.cleanup()
}

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) (err error) {

	if err = mc.watchCancel(ctx); err != nil {
		return
	}
	defer mc.finish()

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

func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := mc.query(query, dargs)
	if err != nil {
		mc.finish()
		return nil, err
	}
	rows.finish = mc.finish
	return rows, err
}

func (mc *mysqlConn) query(query string, args []driver.Value) (*textRows, error) {
	handleOk := mc.clearResult()

	// send command
	err := mc.writeCommandPacketStr(comQuery, query)
	if err != nil {
		return nil, mc.markBadConn(err)
	}

	// read result
	var resLen int
	resLen, err = handleOk.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := new(textRows)
	rows.mc = mc

	if resLen == 0 {
		// TODO: handle empty result set
	}
	rows.rs.columns, err = mc.readColumns(resLen)
	return rows, err

}

func (mc *mysqlConn) syncSequence() {
	// TODO: implement
	// sync sequence number if compression is enabled
}
