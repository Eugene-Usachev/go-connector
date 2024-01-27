package pipe

import (
	"github.com/Eugene-Usachev/fastbytes"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
	"github.com/Eugene-Usachev/go-connector/internal/scheme"
	"github.com/goccy/go-json"
	"log"
	"net"
	"sync"
	"time"
)

func getSize(len int) int {
	if len < 65535 {
		return 2
	}
	return 6
}

func appendSlice(dst []byte, size int, slice []byte) []byte {
	if size == 2 {
		dst = append(dst, byte(len(slice)), byte(len(slice)>>8))
	} else {
		dst = append(dst, 255, 255, byte(len(slice)), byte(len(slice)>>8), byte(len(slice)>>16), byte(len(slice)>>24))
	}
	return append(dst, slice...)
}

func appendKey(dst []byte, slice []byte) []byte {
	if len(slice) > 65535 {
		log.Fatalln("[NimbleDB] Key is too long! Key length must be less than 64 KB!")
	}
	dst = append(dst, byte(len(slice)), byte(len(slice)>>8))
	return append(dst, slice...)
}

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
	if cfg.MaxWriteBufferSize > bufferSize {
		cfg.MaxWriteBufferSize = bufferSize
	} else if cfg.MaxWriteBufferSize < 4 {
		cfg.MaxWriteBufferSize = bufferSize
	}
	pipe := &Pipe{
		writeBuf:            make([]byte, 4, cfg.MaxWriteBufferSize),
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
			if l > pipe.maxWriteBufferSize {
				if len(data) > pipe.maxWriteBufferSize {
					pipe.writeBuf = append(pipe.writeBuf, data...)
					pipe.execPipe()
					pipe.writeBuf = make([]byte, 4, pipe.maxWriteBufferSize)
					pipe.m.Unlock()
					continue
				} else {
					pipe.execPipe()
				}
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

func (pipe *Pipe) CreateTableInMemory(name []byte, scheme *scheme.Scheme, isItLogging bool) chan Res {
	if len(name) > 65535 {
		log.Fatalln("[NimbleDB] table name is too long. Name length must be less than 64 KB!")
	}

	if scheme != nil {
		schemeJson, err := json.Marshal(scheme)
		if err != nil {
			log.Fatalln("[NimbleDB] cannot parse scheme to create table with name", fastbytes.B2S(name), "! Error:", err.Error())
		}
		if len(schemeJson) > 65355 {
			log.Fatalln("[NimbleDB] scheme is too large! Scheme length after marshal must be less than 64KB!")
		}

		length := len(name) + 4 + len(schemeJson)
		pipe.queue <- append(
			append([]byte{byte(length), byte(length >> 8), constants.CreateTableInMemory, boolToByte(isItLogging), byte(len(schemeJson)), byte(len(schemeJson) >> 8)},
				schemeJson...),
			name...)
	} else {
		length := len(name) + 4
		pipe.queue <- append(
			[]byte{byte(length), byte(length >> 8), constants.CreateTableInMemory, boolToByte(isItLogging), 0, 0},
			name...)
	}
	//size := 512
	//pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateTableInMemory, byte(size), byte(size >> 8), boolToByte(isItLogging)}, name...)
	return pipe.Result
}

func (pipe *Pipe) CreateTableCache(name []byte, scheme *scheme.Scheme, cacheDuration uint64, isItLogging bool) chan Res {
	if len(name) > 65535 {
		log.Fatalln("[NimbleDB] table name is too long. Name length must be less than 64 KB!")
	}
	if scheme != nil {
		schemeJson, err := json.Marshal(scheme)
		if err != nil {
			log.Fatalln("[NimbleDB] cannot parse scheme to create table with name", fastbytes.B2S(name), "! Error:", err.Error())
		}
		if len(schemeJson) > 65355 {
			log.Fatalln("[NimbleDB] scheme is too large! Scheme length after marshal must be less than 64KB!")
		}

		length := len(name) + 12 + len(schemeJson)
		pipe.queue <- append(append([]byte{byte(length), byte(length >> 8), constants.CreateTableCache, boolToByte(isItLogging),
			byte(cacheDuration), byte(cacheDuration >> 8), byte(cacheDuration >> 16), byte(cacheDuration >> 24), byte(cacheDuration >> 32),
			byte(cacheDuration >> 40), byte(cacheDuration >> 48), byte(cacheDuration >> 56),
			byte(len(schemeJson)), byte(len(schemeJson) >> 8)}, schemeJson...), name...)
	} else {
		length := uint16(len(name)) + 12
		pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateTableCache, boolToByte(isItLogging),
			byte(cacheDuration), byte(cacheDuration >> 8), byte(cacheDuration >> 16), byte(cacheDuration >> 24), byte(cacheDuration >> 32),
			byte(cacheDuration >> 40), byte(cacheDuration >> 48), byte(cacheDuration >> 56), 0, 0}, name...)
	}
	return pipe.Result
}

func (pipe *Pipe) CreateTableOnDisk(name []byte, scheme *scheme.Scheme) chan Res {
	if len(name) > 65535 {
		log.Fatalln("[NimbleDB] table name is too long. Name length must be less than 64 KB!")
	}
	if scheme != nil {
		schemeJson, err := json.Marshal(scheme)
		if err != nil {
			log.Fatalln("[NimbleDB] cannot parse scheme to create table with name", fastbytes.B2S(name), "! Error:", err.Error())
		}
		if len(schemeJson) > 65355 {
			log.Fatalln("[NimbleDB] scheme is too large! Scheme length after marshal must be less than 64KB!")
		}

		length := len(name) + 3 + len(schemeJson)
		pipe.queue <- append(append([]byte{byte(length), byte(length >> 8), constants.CreateTableOnDisk,
			byte(len(schemeJson)), byte(len(schemeJson) >> 8)}, schemeJson...), name...)

	} else {
		length := uint16(len(name)) + 3
		pipe.queue <- append([]byte{byte(length), byte(length >> 8), constants.CreateTableOnDisk, 0, 0}, name...)
	}
	return pipe.Result
}

func (pipe *Pipe) GetTablesNames() chan Res {
	pipe.queue <- []byte{1, 0, constants.GetTablesNames}
	return pipe.Result
}

func (pipe *Pipe) Insert(key []byte, value []byte, tableId uint16) chan Res {
	keyLength := len(key)
	valueLength := len(value)
	length := 5 + keyLength + valueLength
	lengthSize := getSize(length)

	slice := make([]byte, 0, length+lengthSize)
	if length < 65535 {
		slice = append(slice, byte(length), byte(length>>8), constants.Insert, byte(tableId), byte(tableId>>8))
	} else {
		slice = append(slice, 255, 255, byte(length), byte(length>>8), byte(length>>16), byte(length>>24), constants.Insert, byte(tableId), byte(tableId>>8))
	}

	slice = appendKey(slice, key)
	slice = append(slice, value...)

	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Set(key []byte, value []byte, tableId uint16) chan Res {
	keyLength := len(key)
	valueLength := len(value)
	length := 5 + keyLength + valueLength
	lengthSize := getSize(length)

	slice := make([]byte, 0, length+lengthSize)
	if length < 65535 {
		slice = append(slice, byte(length), byte(length>>8), constants.Set, byte(tableId), byte(tableId>>8))
	} else {
		slice = append(slice, 255, 255, byte(length), byte(length>>8), byte(length>>16), byte(length>>24), constants.Set, byte(tableId), byte(tableId>>8))
	}

	slice = appendKey(slice, key)
	slice = append(slice, value...)

	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Get(key []byte, tableId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Get, byte(tableId), byte(tableId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}

// TODO r
func (pipe *Pipe) GetAndResetCacheTime(key []byte, tableId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.GetAndResetCacheTime, byte(tableId), byte(tableId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}

func (pipe *Pipe) Delete(key []byte, tableId uint16) chan Res {
	length := uint16(len(key)) + 3
	slice := make([]byte, 0, length+2)
	slice = append(slice, byte(length), byte(length>>8), constants.Delete, byte(tableId), byte(tableId>>8))
	slice = append(slice, key...)
	pipe.queue <- slice
	return pipe.Result
}
