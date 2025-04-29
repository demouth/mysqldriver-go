package mysqldriver

import (
	"crypto/sha256"
	"errors"
)

func (mc *mysqlConn) auth(authData []byte, plugin string) ([]byte, error) {
	switch plugin {
	case "caching_sha2_password":
		authResp := scrambleSHA256Password(authData, mc.cfg.Passwd)
		return authResp, nil
	case "mysql_native_password":
		authResp := scramblePassword(authData[:20], mc.cfg.Passwd)
		return authResp, nil
	default:
		return nil, errors.New("this authentication plugin is not supported")
	}
}

func scramblePassword(scramble []byte, password string) []byte {
	return []byte{}
}

func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func (mc *mysqlConn) handleAuthResult(oldAuthData []byte, plugin string) error {
	authData, newPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}

	if newPlugin != "" {
		// TODO
	}

	switch plugin {
	// https://dev.mysql.com/blog-archive/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	case "caching_sha2_password":
		switch len(authData) {
		case 1:
			switch authData[0] {
			case cachingSha2PasswordFastAuthSuccess:
				if err = mc.resultUnchanged().readResultOK(); err == nil {
					return nil // auth successful
				}
			default:
				return errors.New("malformed packet")
			}
		default:
			return errors.New("malformed packet")
		}
	default:
		return nil
	}
	return err
}
