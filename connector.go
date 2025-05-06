package mysqldriver

import (
	"context"
	"database/sql/driver"
	"net"
	"os"
	"strconv"
)

type connector struct {
	cfg               *Config
	encodedAttributes string
}

func encodeConnectionAttributes(cfg *Config) string {
	connAttrsBuf := make([]byte, 0)

	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrClientName)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrClientNameValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrOS)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrOSValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPlatform)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPlatformValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPid)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, strconv.Itoa(os.Getpid()))
	serverHost, _, _ := net.SplitHostPort(cfg.Addr)
	if serverHost != "" {
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrServerHost)
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, serverHost)
	}

	// TODO: user-defined connection attributes

	return string(connAttrsBuf)
}

func newConnector(cfg *Config) *connector {
	encodedAttributes := encodeConnectionAttributes(cfg)
	return &connector{
		cfg:               cfg,
		encodedAttributes: encodedAttributes,
	}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error

	mc := &mysqlConn{
		cfg:       c.cfg,
		closech:   make(chan struct{}),
		connector: c,
	}

	dctx := ctx

	dialsLock.RLock()
	dial, ok := dials[mc.cfg.Net]
	dialsLock.RUnlock()
	if ok {
		_ = dial // TODO: use dial
	} else {
		nd := net.Dialer{}
		mc.netConn, err = nd.DialContext(dctx, mc.cfg.Net, mc.cfg.Addr)
	}
	if err != nil {
		return nil, err
	}
	mc.rawConn = mc.netConn

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			return nil, err
		}
	}

	mc.startWatcher()
	if err := mc.watchCancel(ctx); err != nil {
		mc.cleanup()
		return nil, err
	}
	defer mc.finish()

	mc.buf = newBuffer()

	authData, plugin, err := mc.readHandshakePacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	if plugin == "" {
		plugin = "mysql_native_password"
	}

	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		return nil, err
	}

	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		mc.cleanup()
		return nil, err
	}

	if err = mc.handleAuthResult(authData, plugin); err != nil {
		mc.cleanup()
		return nil, err
	}

	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	}

	return mc, nil
}

func (c *connector) Driver() driver.Driver {
	return &MySQLDriver{}
}
