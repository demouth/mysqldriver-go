package mysqldriver

import (
	"errors"
	"io"
)

const defaultBufSize = 4096
const maxCachedBufSize = 256 * 1024

type readerFunc func([]byte) (int, error)

type buffer struct {
	buf       []byte
	cachedBuf []byte
}

func newBuffer() buffer {
	return buffer{
		cachedBuf: make([]byte, defaultBufSize),
	}
}

func (b *buffer) busy() bool {
	return len(b.buf) > 0
}

func (b *buffer) fill(need int, r readerFunc) error {

	dest := b.cachedBuf

	// grow buffer if needed
	if need > len(dest) {
		dest = make([]byte, ((need/defaultBufSize)+1)*defaultBufSize)

		// cache the buffer if it's small enough
		if len(dest) <= maxCachedBufSize {
			b.cachedBuf = dest
		}
	}

	n := len(b.buf)
	copy(dest[:n], b.buf)

	for {
		nn, err := r(dest[n:])
		n += nn
		if err != nil && n < need {
			continue
		}
		b.buf = dest[:n]
		if err == io.EOF {
			if n < need {
				err = io.ErrUnexpectedEOF
			} else {
				err = nil
			}
		}
		return err
	}
}

func (b *buffer) readNext(need int, r readerFunc) ([]byte, error) {
	if len(b.buf) < need {
		if err := b.fill(need, r); err != nil {
			return nil, err
		}
	}
	data := b.buf[:need]
	b.buf = b.buf[need:]
	return data, nil
}

func (b *buffer) takeBuffer(length int) ([]byte, error) {
	if b.busy() {
		return nil, errors.New("busy buffer")
	}
	if length <= len(b.cachedBuf) {
		return b.cachedBuf[:length], nil
	}
	if length < maxCachedBufSize {
		b.cachedBuf = make([]byte, length)
		return b.cachedBuf, nil
	}
	return make([]byte, length), nil
}

func (b *buffer) takeSmallBuffer(length int) ([]byte, error) {
	if b.busy() {
		return nil, errors.New("busy buffer")
	}
	return b.cachedBuf[:length], nil
}
func (b *buffer) takeCompleteBuffer() ([]byte, error) {
	if b.busy() {
		return nil, errors.New("busy buffer")
	}
	return b.cachedBuf, nil
}

func (b *buffer) store(buf []byte) {
	if cap(buf) <= maxCachedBufSize && cap(buf) > cap(b.cachedBuf) {
		b.cachedBuf = buf[:cap(buf)]
	}
}
