package mysqldriver

import "io"

type readerFunc func([]byte) (int, error)

type buffer struct {
	buf []byte
}

func newBuffer() buffer {
	return buffer{}
}
func (b *buffer) fill(need int, r readerFunc) error {
	dest := make([]byte, need)

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
	return make([]byte, length), nil
}
