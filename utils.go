package mysqldriver

import (
	"database/sql/driver"
	"encoding/binary"
	"io"
)

func getUint24(data []byte) int {
	return int(data[2])<<16 | int(data[1])<<8 | int(data[0])
}

// encodes a uint64 value and appends it to the given bytes slice
func appendLengthEncodedInteger(b []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(b, byte(n))

	case n <= 0xffff:
		b = append(b, 0xfc)
		return binary.LittleEndian.AppendUint16(b, uint16(n))

	case n <= 0xffffff:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	b = append(b, 0xfe)
	return binary.LittleEndian.AppendUint64(b, n)
}
func putUint24(data []byte, n int) {
	data[2] = byte(n >> 16)
	data[1] = byte(n >> 8)
	data[0] = byte(n)
}

func appendLengthEncodedString(b []byte, s string) []byte {
	b = appendLengthEncodedInteger(b, uint64(len(s)))
	return append(b, s...)
}

// returns the number read, whether the value is NULL and the number of bytes read
func readLengthEncodedInteger(b []byte) (uint64, bool, int) {
	// See issue #349
	if len(b) == 0 {
		return 0, true, 1
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

	// 252: value of following 2
	case 0xfc:
		return uint64(binary.LittleEndian.Uint16(b[1:])), false, 3

	// 253: value of following 3
	case 0xfd:
		return uint64(getUint24(b[1:])), false, 4

	// 254: value of following 8
	case 0xfe:
		return uint64(binary.LittleEndian.Uint64(b[1:])), false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	// TODO: handle named values
	return dargs, nil
}
func readLengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := readLengthEncodedInteger(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}
func skipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := readLengthEncodedInteger(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}
