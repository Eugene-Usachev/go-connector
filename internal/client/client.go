package client

import (
	"errors"
	"fmt"
	"github.com/Eugene-Usachev/fastbytes"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
	"github.com/Eugene-Usachev/go-connector/internal/pipe"
	"github.com/Eugene-Usachev/go-connector/internal/result"
	"github.com/Eugene-Usachev/go-connector/internal/scheme"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	hierarchy     [][]string
	shardMetadata []atomic.Uint32

	writePool      []*pipe.Pipe
	writePoolCount atomic.Uint64
	readPool       []*pipe.Pipe
	readPoolCount  atomic.Uint64

	par uint64
}

type Config struct {
	Host               string
	Port               string
	Password           string
	Par                int
	MaxQueueSize       int
	MaxWriteBufferSize int
}

// NewClient creates a new client and sets it up.
func NewClient(cfg *Config) (*Client, error) {
	connPool := &sync.Pool{
		New: func() interface{} {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", cfg.Host, cfg.Port))
			if err != nil {
				log.Println("[NimbleDB] Error connecting to the database: ", err)
				return nil
			}
			if len(cfg.Password) > 0 {
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err = conn.Write(fastbytes.S2B(cfg.Password))
				if err != nil {
					log.Println("[NimbleDB] Error connecting to the database: ", err)
					return nil
				}

				status := make([]byte, 1)
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, err = conn.Read(status[:])
				if err != nil {
					log.Println("[NimbleDB] Error connecting to the database: ", err)
					return nil
				}
				if status[0] != constants.Done {
					log.Println("[NimbleDB] Error connecting to the database. Status: ", string(status))
					return nil
				}
			}

			return conn
		},
	}

	pipeCfg := &pipe.Config{
		ConnPool:           connPool,
		MaxQueueSize:       cfg.MaxQueueSize,
		MaxWriteBufferSize: cfg.MaxWriteBufferSize,
	}

	c := &Client{
		hierarchy:     nil,
		shardMetadata: nil,

		writePool:      make([]*pipe.Pipe, cfg.Par),
		writePoolCount: atomic.Uint64{},
		readPool:       make([]*pipe.Pipe, cfg.Par),
		readPoolCount:  atomic.Uint64{},
		par:            uint64(cfg.Par),
	}

	for i := 0; i < cfg.Par; i++ {
		pipeImpl1 := pipe.NewPipe(pipeCfg)
		pipeImpl2 := pipe.NewPipe(pipeCfg)
		go pipeImpl1.Start()
		go pipeImpl2.Start()
		//time.Sleep(17 * time.Microsecond)
		//go pipeImpl1.StartTimer()
		//go pipeImpl2.StartTimer()

		c.writePool[i] = pipeImpl1
		c.readPool[i] = pipeImpl2
	}

	go func() {
		for {
			time.Sleep(100 * time.Microsecond)
			for i := 0; i < cfg.Par; i++ {
				go c.readPool[i].ExecPipe()
				c.readPoolCount.Store(0)
				go c.writePool[i].ExecPipe()
				c.writePoolCount.Store(0)
			}
		}
	}()

	tries := 0
	{
		conn := c.readPool[0]
	connect:
		pingRes := conn.Ping()
		shardMetadataRes := conn.GetShardMetadata()
		hierarchyRes := conn.GetHierarchy()

		res := <-pingRes
		if res.Err != nil || len(res.Slice) != 2 || res.Slice[1] != constants.Ping {
			time.Sleep(time.Millisecond * 200)
			tries++
			if tries == 50 {
				return nil, errors.New("can't connect to the server")
			}
			<-shardMetadataRes
			<-hierarchyRes
			goto connect
		}

		res = <-shardMetadataRes
		if res.Err != nil || len(res.Slice) != 65536*2+1 || res.Slice[0] != constants.Done {
			time.Sleep(time.Millisecond * 200)
			tries++
			if tries == 50 {
				return nil, errors.New("can't get shard metadata")
			}
			<-hierarchyRes
			goto connect
		}
		c.shardMetadata = make([]atomic.Uint32, 65536)
		for i := 0; i < 65536; i++ {
			numberU16 := fastbytes.B2U16(res.Slice[i*2+1 : i*2+3])
			c.shardMetadata[i].Store(uint32(numberU16))
		}

		res = <-hierarchyRes
		if res.Err != nil || len(res.Slice) < 4 || res.Slice[0] != constants.Done {
			time.Sleep(time.Millisecond * 200)
			tries++
			if tries == 50 {
				return nil, errors.New("can't get hierarchy")
			}
			goto connect
		}

		offset := 1
		nameLen := uint16(0)
		nameLenI := int(nameLen)
		var node []string
		hierarchy := make([][]string, 0, 1)
		numberOfMachines := uint8(0)
		for offset < len(res.Slice) {
			if numberOfMachines == 0 {
				numberOfMachines = res.Slice[offset]
				offset += 1
				if node != nil {
					hierarchy = append(hierarchy, node)
				}
				node = make([]string, 0, numberOfMachines)
			}

			nameLen = fastbytes.B2U16(res.Slice[offset : offset+2])
			nameLenI = int(nameLen)
			offset += 2

			name := fastbytes.B2S(res.Slice[offset : offset+nameLenI])
			offset += nameLenI
			node = append(node, name)
		}
		hierarchy = append(hierarchy, node)
		c.hierarchy = make([][]string, len(hierarchy))
		copy(c.hierarchy, hierarchy[:])
	}

	return c, nil
}

func (c *Client) CallReadFunc(f func(conn *pipe.Pipe) chan pipe.Res) ([]byte, error) {
	retryCount := 0
retry:
	count := c.readPoolCount.Add(1)
	conn := c.readPool[count%c.par]
	ch := f(conn)
	var res pipe.Res
	res = <-ch
	if res.Err != nil {
		retryCount++
		if retryCount != 15 {
			goto retry
		}
		return nil, res.Err
	}
	if len(res.Slice) < 1 {
		return nil, fmt.Errorf("empty response")
	}
	err := result.DefineError(res.Slice[0])
	if err != nil {
		return nil, err
	}
	return res.Slice[:], nil
}

func (c *Client) CallWriteFunc(f func(conn *pipe.Pipe) chan pipe.Res) ([]byte, error) {
	count := c.readPoolCount.Add(1)
	conn := c.readPool[count%c.par]
	ch := f(conn)
	var res pipe.Res
	res = <-ch
	if res.Err != nil {
		return nil, res.Err
	}
	if len(res.Slice) < 1 {
		return nil, fmt.Errorf("empty response")
	}
	err := result.DefineError(res.Slice[0])
	if err != nil {
		return nil, err
	}
	return res.Slice[:], nil
}

// Ping returns true if the connection is alive and false otherwise.
func (c *Client) Ping() bool {
	res, err := c.CallReadFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Ping()
	})

	if len(res) != 2 {
		return false
	}
	if err != nil || res[1] != constants.Ping {
		return false
	}
	return true

	// TODO r
	//conn := <-c.pool
	//ch := conn.Ping()
	//c.pool <- conn
	//res := <-ch
	//return res.Err == nil
}

func (c *Client) CreateTableInMemory(name []byte, scheme *scheme.Scheme, isItLogging bool) result.Result[uint16] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.CreateTableInMemory(name, scheme, isItLogging)
	})
	if err != nil {
		return result.Result[uint16]{
			Value: 0,
			Err:   err,
			IsOk:  false,
		}
	}
	return result.Result[uint16]{
		Value: fastbytes.B2U16(res[1:3]),
		Err:   nil,
		IsOk:  true,
	}
}

// cacheDuration is a time in minutes.
func (c *Client) CreateTableCache(name []byte, scheme *scheme.Scheme, isItLogging bool, cacheDuration uint64) result.Result[uint16] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.CreateTableCache(name, scheme, cacheDuration, isItLogging)
	})
	if err != nil {
		return result.Result[uint16]{
			Value: 0,
			Err:   err,
			IsOk:  false,
		}
	}
	return result.Result[uint16]{
		Value: fastbytes.B2U16(res[1:3]),
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) CreateTableOnDisk(name []byte, scheme *scheme.Scheme) result.Result[uint16] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.CreateTableOnDisk(name, scheme)
	})
	if err != nil {
		return result.Result[uint16]{
			Value: 0,
			Err:   err,
			IsOk:  false,
		}
	}
	return result.Result[uint16]{
		Value: fastbytes.B2U16(res[1:3]),
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) GetTablesNames() result.Result[[]string] {
	res, err := c.CallReadFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.GetTablesNames()
	})

	if err != nil {
		return result.Result[[]string]{
			Value: nil,
			Err:   err,
			IsOk:  false,
		}
	}

	offset, length := 0, len(res)
	arr := []string{}

	for {
		if offset == length {
			break
		}
		size := int(fastbytes.B2U16(res[offset : offset+2]))
		arr = append(arr, string(res[offset+2:offset+2+size]))
		offset += 2 + size
	}

	return result.Result[[]string]{
		Value: arr,
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) Insert(key []byte, value []byte, tableId uint16) result.Result[result.Void] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Insert(key, value, tableId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[result.Void]{
			Value: result.Void{},
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[result.Void]{
		Value: result.Void{},
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) Set(key []byte, value []byte, tableId uint16) result.Result[result.Void] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Set(key, value, tableId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[result.Void]{
			Value: result.Void{},
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[result.Void]{
		Value: result.Void{},
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) Get(key []byte, tableId uint16) result.Result[[]byte] {
	res, err := c.CallReadFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Get(key, tableId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[[]byte]{
			Value: nil,
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[[]byte]{
		Value: res[1:],
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) GetAndResetCacheTime(key []byte, tableId uint16) result.Result[[]byte] {
	res, err := c.CallReadFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.GetAndResetCacheTime(key, tableId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[[]byte]{
			Value: nil,
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[[]byte]{
		Value: res[1:],
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) Delete(key []byte, tableId uint16) result.Result[result.Void] {
	res, err := c.CallWriteFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Delete(key, tableId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[result.Void]{
			Value: result.Void{},
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[result.Void]{
		Value: result.Void{},
		Err:   nil,
		IsOk:  true,
	}
}
