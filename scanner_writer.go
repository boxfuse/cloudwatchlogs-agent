package logger

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

type (
	ScannerWriter struct {
		buf     []byte
		max_buf int

		me     sync.Mutex
		closed bool

		splitFunc bufio.SplitFunc
		tokenFunc func(token []byte) error
	}
)

var (
	ExceededBufferSizeLimitError = fmt.Errorf("exceeded buffer size limit")
	WriterClosedError            = fmt.Errorf("cannot write to closed writer")
)

func NewScannerWriter(splitFunc bufio.SplitFunc, max_buf int, tokenFunc func([]byte) error) *ScannerWriter {
	return &ScannerWriter{
		splitFunc: splitFunc,
		tokenFunc: tokenFunc,
		max_buf:   max_buf,
	}
}

func (sc *ScannerWriter) Write(data []byte) (int, error) {

	sc.me.Lock()
	defer sc.me.Unlock()

	if sc.closed {
		return 0, WriterClosedError
	}

	data_len := len(data)

	if sc.buf != nil {
		data = append(sc.buf, data...)
		sc.buf = nil
	}

	for len(data) > 0 {

		adv, token, err := sc.splitFunc(data, false)
		if err != nil {
			return 0, err
		}

		if token == nil {
			if adv == 0 {
				// read more requests are buffered until next write
				if len(sc.buf)+len(data) > sc.max_buf {
					return 0, ExceededBufferSizeLimitError
				}
				sc.buf = append(sc.buf, data...)
				return data_len, nil
			}
		} else if err := sc.tokenFunc(token); err != nil {
			return 0, err
		}

		if adv > 0 {
			data = data[adv:]
		}

	}

	return data_len, nil

}

func (sc *ScannerWriter) Flush() error {

	sc.me.Lock()
	defer sc.me.Unlock()

	return sc.flush()

}

func (sc *ScannerWriter) flush() error {

	if sc.closed {
		return WriterClosedError
	}

	if len(sc.buf) == 0 {
		sc.buf = nil
		return nil
	}

	_, token, err := sc.splitFunc(sc.buf, true)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if len(token) > 0 {
		if err := sc.tokenFunc(token); err != nil {
			return err
		}
	}

	sc.buf = nil

	return nil

}

func (sc *ScannerWriter) Close() error {

	sc.me.Lock()
	defer sc.me.Unlock()

	if sc.closed {
		return WriterClosedError
	}

	if err := sc.flush(); err != nil {
		return err
	}

	sc.closed = true
	sc.buf = nil
	sc.splitFunc = nil
	sc.tokenFunc = nil

	return nil

}
