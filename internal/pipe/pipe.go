package pipe

import (
	"github.com/Eugene-Usachev/fastbytes"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
	"net"
	"sync"
	"time"
)

const (
	bufferSize = 256 * 1024
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
	connPool *sync.Pool

	readBuf []byte

	Result chan Res

	queue              chan []byte
	queueSize          int
	maxQueueSize       int
	maxWriteBufferSize int

	m *sync.Mutex

	wasExecutedLastTime bool
}

type Config struct {
	ConnPool           *sync.Pool
	MaxQueueSize       int
	MaxWriteBufferSize int
}

func NewPipe(cfg *Config) *Pipe {
	pipe := &Pipe{
		writeBuf:            make([]byte, 4, bufferSize),
		readBuf:             make([]byte, bufferSize),
		Result:              make(chan Res),
		queue:               make(chan []byte),
		maxQueueSize:        cfg.MaxQueueSize,
		connPool:            cfg.ConnPool,
		maxWriteBufferSize:  cfg.MaxWriteBufferSize,
		m:                   &sync.Mutex{},
		queueSize:           0,
		wasExecutedLastTime: false,
	}
	return pipe
}

func (pipe *Pipe) Start() {
	for {
		select {
		case data := <-pipe.queue:
			pipe.m.Lock()
			l := len(pipe.writeBuf) + len(data)
			if l > bufferSize || l > pipe.maxWriteBufferSize {
				pipe.execPipe()
			}

			pipe.writeBuf = append(pipe.writeBuf, data...)

			pipe.queueSize++
			if pipe.queueSize == pipe.maxQueueSize {
				pipe.execPipe()
			}

			pipe.m.Unlock()
		}
	}
}

func (pipe *Pipe) StartTimer() {
	for {
		time.Sleep(100 * time.Microsecond)
		pipe.m.Lock()
		if pipe.wasExecutedLastTime {
			pipe.wasExecutedLastTime = false
		} else {
			if pipe.queueSize != 0 {
				pipe.execPipe()
			}
		}
		pipe.m.Unlock()
	}
}

func (pipe *Pipe) execPipe() {
	pipe.wasExecutedLastTime = true
	writeBufLen := len(pipe.writeBuf)
	if writeBufLen == 0 {
		return
	}
	conn := pipe.connPool.Get().(net.Conn)
	defer pipe.connPool.Put(conn)
	pipe.writeBuf[0], pipe.writeBuf[1], pipe.writeBuf[2], pipe.writeBuf[3] = byte(writeBufLen), byte(writeBufLen>>8), byte(writeBufLen>>16), byte(writeBufLen>>24)
	_, err := conn.Write(pipe.writeBuf[:writeBufLen])
	pipe.writeBuf = pipe.writeBuf[:4]
	if err != nil {
		for i := 0; i < pipe.queueSize; i++ {
			pipe.Result <- Res{
				Slice: nil,
				Err:   err,
			}
		}
		pipe.queueSize = 0
		return
	}
	l, err := conn.Read(pipe.readBuf[:])
	if err != nil {
		for i := 0; i < pipe.queueSize; i++ {
			pipe.Result <- Res{
				Slice: nil,
				Err:   err,
			}
		}
		pipe.queueSize = 0
		return
	}
	offset := 0
	size := 0
	have := 0

	for pipe.queueSize > 0 {
		have = l - offset
		if have < 3 {
			if have != 0 {
				copy(pipe.readBuf[0:have], pipe.readBuf[offset:offset+have])
			}
			l, err = conn.Read(pipe.readBuf[have:])
			if err != nil {
				for i := 0; i < pipe.queueSize; i++ {
					pipe.Result <- Res{
						Slice: nil,
						Err:   err,
					}
				}
				pipe.queueSize = 0
				return
			}
			offset = 0
			l += have
		}

		size = int(fastbytes.B2U16(pipe.readBuf[offset : offset+2]))
		offset += 2
		if size == 65535 {
			size = int(fastbytes.B2U32(pipe.readBuf[offset : offset+4]))
			offset += 4
		}

		for have = l - offset; have < size; have = l - offset {
			copy(pipe.readBuf[0:have], pipe.readBuf[offset:offset+have])
			l, err = conn.Read(pipe.readBuf[have:])
			if err != nil {
				for i := 0; i < pipe.queueSize; i++ {
					pipe.Result <- Res{
						Slice: nil,
						Err:   err,
					}
				}
				pipe.queueSize = 0
				return
			}
			offset = 0
			l += have
		}

		pipe.Result <- Res{
			Slice: pipe.readBuf[offset : offset+size],
			Err:   nil,
		}
		offset += size

		pipe.queueSize--
	}
}

func (pipe *Pipe) Ping() chan Res {
	pipe.queue <- []byte{1, 0, constants.Ping}
	return pipe.Result
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func (pipe *Pipe) CreateSpaceInMemory(size uint16, name []byte, isItLogging bool) chan Res {
	length := uint16(len(name)) + 4
	pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateSpaceInMemory, byte(size), byte(size >> 8), boolToByte(isItLogging)}, name...)
	return pipe.Result
}

func (pipe *Pipe) CreateSpaceCache(size uint16, name []byte, cacheDuration uint64, isItLogging bool) chan Res {
	length := uint16(len(name)) + 12
	pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateSpaceCache, byte(size), byte(size >> 8), boolToByte(isItLogging),
		byte(cacheDuration), byte(cacheDuration >> 8), byte(cacheDuration >> 16), byte(cacheDuration >> 24), byte(cacheDuration >> 32), byte(cacheDuration >> 40), byte(cacheDuration >> 48), byte(cacheDuration >> 56)}, name...)
	return pipe.Result
}

func (pipe *Pipe) CreateSpaceOnDisk(size uint16, name []byte) chan Res {
	length := uint16(len(name)) + 3
	pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateSpaceOnDisk, byte(size), byte(size >> 8)}, name...)
	return pipe.Result
}

func (pipe *Pipe) GetSpacesNames() chan Res {
	pipe.queue <- []byte{1, 0, constants.GetSpacesNames}
	return pipe.Result
}

func (pipe *Pipe) Insert(key []byte, value []byte, spaceId uint16) chan Res {
	keyLength := uint16(len(key))
	valueLength := uint16(len(value))
	// spaceId (2) + key length (2)  + msg type(1) = 7
	length := 5 + keyLength + valueLength
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Insert, byte(spaceId), byte(spaceId>>8), byte(keyLength), byte(keyLength>>8))
	slice = append(slice, key...)
	slice = append(slice, value...)
	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Set(key []byte, value []byte, spaceId uint16) chan Res {
	keyLength := uint16(len(key))
	valueLength := uint16(len(value))
	// spaceId (2) + key length (2)  + msg type(1) = 7
	length := 5 + keyLength + valueLength
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Set, byte(spaceId), byte(spaceId>>8), byte(keyLength), byte(keyLength>>8))
	slice = append(slice, key...)
	slice = append(slice, value...)
	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Get(key []byte, spaceId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Get, byte(spaceId), byte(spaceId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) GetAndResetCacheTime(key []byte, spaceId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.GetAndResetCacheTime, byte(spaceId), byte(spaceId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Delete(key []byte, spaceId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Delete, byte(spaceId), byte(spaceId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}
