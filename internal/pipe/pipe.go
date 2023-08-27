package pipe

import (
	"fmt"
	"github.com/Eugene-Usachev/fastbytes"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
	"net"
	"sync"
	"time"
)

const (
	bufferSize = 64 * 1024
	volume     = 1024
)

type Slice []byte
type Msg struct {
	data []byte
	ch   chan Res
}

type Res struct {
	Slice
	Err error
}

type Pipe struct {
	writeBuf []byte
	conn     net.Conn

	readBuf []byte
	// responseBuf is a buffer for storing a big response message.
	responseBuf []byte

	Result chan Res

	queue     chan []byte
	queueSize int

	m sync.Mutex
}

func NewPipe(host string, port string) (*Pipe, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return nil, err
	}
	return &Pipe{
		writeBuf:    make([]byte, 0, bufferSize),
		readBuf:     make([]byte, bufferSize),
		responseBuf: make([]byte, 0, bufferSize*2),
		Result:      make(chan Res),
		queue:       make(chan []byte),
		m:           sync.Mutex{},
		conn:        conn,
	}, nil
}

func (pipe *Pipe) Start() {
	for {
		select {
		case data := <-pipe.queue:
			pipe.m.Lock()
			l := len(data)
			if len(pipe.writeBuf)+l > bufferSize || pipe.queueSize == volume {
				pipe.ExecPipe()
			}

			if l > 65535 {

			} else {
				pipe.writeBuf = append(pipe.writeBuf, fastbytes.U16ToB(uint16(l))...)
			}
			pipe.writeBuf = append(pipe.writeBuf, data...)

			pipe.queueSize++
			pipe.m.Unlock()
		case <-time.After(time.Microsecond * 100):
			pipe.m.Lock()
			if pipe.queueSize != 0 {
				pipe.ExecPipe()
			}
			pipe.m.Unlock()
		}
	}
}

func (pipe *Pipe) ExecPipe() {
	writeBufLen := len(pipe.writeBuf)
	if writeBufLen == 0 {
		return
	}
	_, err := pipe.conn.Write(pipe.writeBuf[:writeBufLen])
	if err != nil {
		for i := 0; i < pipe.queueSize; i++ {
			pipe.Result <- Res{
				Slice: nil,
				Err:   err,
			}
		}
		return
	}
	pipe.writeBuf = pipe.writeBuf[:0]

	_, err = pipe.conn.Read(pipe.readBuf[:])
	if err != nil {
		for i := 0; i < pipe.queueSize; i++ {
			pipe.Result <- Res{
				Slice: nil,
				Err:   err,
			}
		}
		return
	}

	offset := 0
	for {
		if pipe.queueSize == 0 {
			break
		}

		size := int(fastbytes.B2U16(pipe.readBuf[offset : offset+2]))
		if size == 65535 {
			size = int(fastbytes.B2U32(pipe.readBuf[offset : offset+4]))
			offset += 4
		} else {
			offset += 2
		}
		if bufferSize-offset < size {
			for bufferSize-offset < size {
				pipe.responseBuf = append(pipe.responseBuf, pipe.readBuf[offset:]...)
				size -= offset
				offset, err = pipe.conn.Read(pipe.readBuf[offset:])
			}
			pipe.Result <- Res{
				Slice: pipe.responseBuf[:],
				Err:   nil,
			}
			pipe.responseBuf = pipe.responseBuf[:0]
		} else {
			pipe.Result <- Res{
				Slice: pipe.readBuf[offset : offset+size],
				Err:   nil,
			}
			offset += size
		}
		pipe.queueSize--
	}
}

func (pipe *Pipe) Write(data []byte) {
	pipe.queue <- data
}

func (pipe *Pipe) Ping() chan Res {
	pipe.queue <- []byte{constants.Ping}
	return pipe.Result
}
