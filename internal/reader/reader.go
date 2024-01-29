package reader

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const BufferSize = math.MaxUint16

type Status uint8

const (
	Ok Status = iota
	All
	Closed
	Error
)

type BufReader struct {
	buf         [BufferSize]byte
	bigBuf      []byte
	reader      io.Reader
	readOffset  int
	writeOffset int
	requestSize int
}

func NewBufReader() *BufReader {
	return &BufReader{
		buf:         [BufferSize]byte{},
		bigBuf:      make([]byte, 0),
		readOffset:  0,
		writeOffset: 0,
		requestSize: 0,
	}
}

func (r *BufReader) SetReader(reader io.Reader) {
	r.reader = reader
}

func readMore(reader *BufReader, needed int) Status {
	if needed > BufferSize {
		reader.bigBuf = make([]byte, needed)
		read := reader.writeOffset - reader.readOffset
		copy(reader.bigBuf[0:read], reader.buf[reader.readOffset:reader.writeOffset])
		reader.writeOffset = 0
		reader.readOffset = 0
		for {
			n, err := reader.reader.Read(reader.bigBuf[read:needed])
			if err != nil {
				fmt.Printf("Read connection error: %v\n", err)
				return Error
			}
			if n == 0 {
				return Closed
			}
			read += n
			if read >= needed {
				return Ok
			}
		}
	}
	if needed > BufferSize-reader.writeOffset {
		left := reader.writeOffset - reader.readOffset
		buf := make([]byte, left)
		copy(buf, reader.buf[reader.readOffset:reader.writeOffset])
		copy(reader.buf[0:left], buf)
		reader.readOffset = 0
		reader.writeOffset = left
	}

	read := 0
	for {
		n, err := reader.reader.Read(reader.buf[reader.writeOffset:])
		if err != nil {
			fmt.Printf("Read connection error: %v\n", err)
			return Error
		}
		if n == 0 {
			return Closed
		}
		reader.writeOffset += n
		read += n
		if read >= needed {
			return Ok
		}
	}
}

func (r *BufReader) ReadRequest() Status {
	r.writeOffset = 0
	status := readMore(r, 4)
	if status != Ok {
		return status
	}
	r.requestSize = int(binary.LittleEndian.Uint32(r.buf[0:4])) - 4
	r.readOffset = 4
	return Ok
}

func (r *BufReader) ReadMessage() ([]byte, Status) {
	if cap(r.bigBuf) > 0 {
		r.bigBuf = make([]byte, 0)
	}
	if r.requestSize == 0 {
		return []byte{}, All
	}
	if r.writeOffset < r.readOffset+2 {
		status := readMore(r, 2)
		if status != Ok {
			return []byte{}, status
		}
	}

	length := int(binary.LittleEndian.Uint16(r.buf[r.readOffset : r.readOffset+2]))
	r.readOffset += 2
	r.requestSize -= 2
	if length == math.MaxUint16 {
		if r.writeOffset < r.readOffset+4 {
			status := readMore(r, 4)
			if status != Ok {
				return []byte{}, status
			}
		}
		length = int(binary.LittleEndian.Uint32(r.buf[r.readOffset : r.readOffset+4]))
		r.readOffset += 4
		r.requestSize -= 4
	}

	if r.writeOffset < r.readOffset+length {
		status := readMore(r, length)
		if status != Ok {
			return []byte{}, status
		}
	}

	r.requestSize -= length
	if length < math.MaxUint16 {
		r.readOffset += length
		return r.buf[r.readOffset-length : r.readOffset], Ok
	}

	return r.bigBuf[:length], Ok
}

func (r *BufReader) ReadMessageWithoutRequest() ([]byte, Status) {
	if cap(r.bigBuf) > 0 {
		r.bigBuf = make([]byte, 0)
	}
	if r.writeOffset < r.readOffset+2 {
		status := readMore(r, 2)
		if status != Ok {
			return []byte{}, status
		}
	}

	length := int(binary.LittleEndian.Uint16(r.buf[r.readOffset : r.readOffset+2]))
	r.readOffset += 2
	if length == math.MaxUint16 {
		if r.writeOffset < r.readOffset+4 {
			status := readMore(r, 4)
			if status != Ok {
				return []byte{}, status
			}
		}
		length = int(binary.LittleEndian.Uint32(r.buf[r.readOffset : r.readOffset+4]))
		r.readOffset += 4
	}

	if r.writeOffset < r.readOffset+length {
		status := readMore(r, length)
		if status != Ok {
			return []byte{}, status
		}
	}

	if length < math.MaxUint16 {
		r.readOffset += length
		return r.buf[r.readOffset-length : r.readOffset], Ok
	}

	return r.bigBuf[:length], Ok
}

func (r *BufReader) Reset() {
	if cap(r.bigBuf) > 0 {
		r.bigBuf = make([]byte, 0)
	}
	r.requestSize = 0
	r.readOffset = 0
	r.writeOffset = 0
}
