package mysqldriver

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	User             string
	Passwd           string
	Net              string // Network (e.g. "tcp", "tcp6", "unix". default: "tcp")
	Addr             string // Address (default: "127.0.0.1:3306" for "tcp" and "/tmp/mysql.sock" for "unix")
	DBName           string
	Loc              *time.Location
	MaxAllowedPacket int

	ParseTime bool

	pubKey   *rsa.PublicKey
	charsets []string
}

func ParseDSN(dsn string) (cfg *Config, err error) {
	cfg = NewConfig()

	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			if i > 0 {

				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {

						// username[:password]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]
						break
					}
				}

				// [protocol[(address)]]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}

				cfg.Net = dsn[j+1 : k]
			}

			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}

			dbname := dsn[i+1 : j]
			if cfg.DBName, err = url.PathUnescape(dbname); err != nil {
				return nil, fmt.Errorf("invalid dbname %q: %w", dbname, err)
			}

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errors.New("invalid DSN: missing the slash separating the database name")
	}

	if err = cfg.normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		key, value, found := strings.Cut(v, "=")
		if !found {
			continue
		}
		switch key {
		case "charset":
			cfg.charsets = strings.Split(value, ",")
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}
		}
	}
	return
}

func (cfg *Config) normalize() error {
	if cfg.Net == "" {
		cfg.Net = "tcp"
	}
	return nil
}

func NewConfig() *Config {
	cfg := &Config{
		MaxAllowedPacket: defaultMaxAllowedPacket,
	}
	return cfg
}
