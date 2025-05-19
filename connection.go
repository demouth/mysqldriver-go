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
	if mc.closed.Load() {
		return nil, driver.ErrBadConn
	}
	err := mc.writeCommandPacketStr(comStmtPrepare, query)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}
			if columnCount > 0 {
				err = mc.readUntilEOF()
			}
		}
	}
	return stmt, err
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if mc.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// TODO: implement interpolateParams
	}

	err := mc.exec(query)
	if err == nil {
		copied := mc.result
		return &copied, err
	}
	return nil, mc.markBadConn(err)
}

func (mc *mysqlConn) exec(query string) error {
	handleOk := mc.clearResult()
	if err := mc.writeCommandPacketStr(comQuery, query); err != nil {
		return mc.markBadConn(err)
	}

	resLen, err := handleOk.readResultSetHeaderPacket()
	if err != nil {
		return err
	}
	if resLen > 0 {
		// columns
		if err := mc.readUntilEOF(); err != nil {
			return err
		}
		// rows
		if err := mc.readUntilEOF(); err != nil {
			return err
		}
	}
	return handleOk.discardResults()
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	return mc.Exec(query, dargs)
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

func (mc *mysqlConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
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

	if mc.closed.Load() {
		return nil, driver.ErrBadConn
	}

	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
	}

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

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	stmt, err := mc.Prepare(query)
	mc.finish()

	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	rows, err := stmt.query(dargs)
	if err != nil {
		stmt.mc.finish()
		return nil, err
	}
	rows.finish = stmt.mc.finish
	return rows, err
}
