package mysqldriver

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
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

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

func (mc *mysqlConn) sendEncryptedPassword(seed []byte, pub *rsa.PublicKey) error {
	enc, err := encryptPassword(mc.cfg.Passwd, seed, pub)
	if err != nil {
		return err
	}
	return mc.writeAuthSwitchPacket(enc)
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
			case cachingSha2PasswordPerformFullAuthentication:
				pubKey := mc.cfg.pubKey
				if pubKey == nil {
					data, err := mc.buf.takeSmallBuffer(4 + 1)
					if err != nil {
						return err
					}
					data[4] = cachingSha2PasswordRequestPublicKey
					err = mc.writePacket(data)
					if err != nil {
						return err
					}
					if data, err = mc.readPacket(); err != nil {
						return err
					}
					if data[0] != iAuthMoreData {
						return fmt.Errorf("unexpected resp from server for caching_sha2_password, perform full authentication")
					}

					block, rest := pem.Decode(data[1:])
					if block == nil {
						return fmt.Errorf("no pem data found, data: %s", rest)
					}
					pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
					if err != nil {
						return err
					}
					pubKey = pkix.(*rsa.PublicKey)
				}
				err = mc.sendEncryptedPassword(oldAuthData, pubKey)
				if err != nil {
					return err
				}
				return mc.resultUnchanged().readResultOK()
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
