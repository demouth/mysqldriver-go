package mysqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"sync"
)

type MySQLDriver struct{}
type DialContextFunc func(ctx context.Context, addr string) (net.Conn, error)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialContextFunc
)

func (d MySQLDriver) Open(dsn string) (driver.Conn, error) {
	return nil, nil
}

var driverName = "mysqldriver"

func init() {
	if driverName != "" {
		sql.Register(driverName, &MySQLDriver{})
	}
}

func (d MySQLDriver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}
	return newConnector(cfg), nil
}
