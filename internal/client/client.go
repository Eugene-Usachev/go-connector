// TODO r
//package client

//
//import (
//	"github.com/Eugene-Usachev/go-connector/internal/pipe"
//	"log"
//	"sync"
//)
//
//type Client struct {
//	pool sync.Pool
//}
//
//func NewClient(host, port string) *Client {
//	c := &Client{
//		pool: sync.Pool{
//			New: func() interface{} {
//				pipeImpl, err := pipe.NewPipe(host, port)
//				if err != nil {
//					log.Println("[NimbleDB] Error creating pipe: ", err)
//				}
//				go pipeImpl.Start()
//				return pipeImpl
//			},
//		},
//	}
//
//	for i := 0; i < 64; i++ {
//		c.pool.Put(c.pool.Get())
//	}
//
//	return c
//}
//
//// Ping returns true if the connection is alive and false otherwise.
//func (c *Client) Ping() bool {
//	conn := c.pool.Get().(*pipe.Pipe)
//	defer c.pool.Put(conn)
//	return conn.Ping().Value
//}
//
////// CreateSpace creates a new space in the database. CreateSpace returns the ID of the space.
////func (c *Client) CreateSpace(engineType constants.SpaceEngineType, name string, size uint32) result.Result[uint16] {
////	conn := c.pool.Get().(*conn.Connection)
////	defer c.pool.Put(conn)
////	return conn.CreateSpace(engineType, name, size)
////}
////
////// GetSpacesNames returns the names of all spaces in the database.
////func (c *Client) GetSpacesNames() result.Result[[]string] {
////	conn := c.pool.Get().(*conn.Connection)
////	defer c.pool.Put(conn)
////	return conn.GetSpacesNames()
////}
////
////// Get returns the value of the key. The value is raw bytes.
////func (c *Client) Get(key []byte, spaceId uint16) result.Result[[]byte] {
////	conn := c.pool.Get().(*conn.Connection)
////	defer c.pool.Put(conn)
////	return conn.Get(key, spaceId)
////}
////
////// Set sets a value by a key. It will return void value, check Result.IsOk.
////func (c *Client) Set(key []byte, value []byte, spaceId uint16) result.Result[result.Void] {
////	conn := c.pool.Get().(*conn.Connection)
////	defer c.pool.Put(conn)
////	return conn.Set(key, value, spaceId)
////}

package client

import (
	"fmt"
	"github.com/Eugene-Usachev/fastbytes"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
	"github.com/Eugene-Usachev/go-connector/internal/pipe"
	"github.com/Eugene-Usachev/go-connector/internal/result"
	"log"
	"net"
	"sync"
	"time"
)

type Client struct {
	pool chan *pipe.Pipe
}

type Config struct {
	Host               string
	Port               string
	Par                int
	MaxQueueSize       int
	MaxWriteBufferSize int
}

func NewClient(cfg *Config) *Client {
	c := &Client{
		pool: make(chan *pipe.Pipe, cfg.Par),
	}

	connPool := &sync.Pool{
		New: func() interface{} {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", cfg.Host, cfg.Port))
			if err != nil {
				log.Println("[NimbleDB] Error connecting to the database: ", err)
				return nil
			}
			return conn
		},
	}

	pipeCfg := &pipe.Config{
		ConnPool:           connPool,
		MaxQueueSize:       cfg.MaxQueueSize,
		MaxWriteBufferSize: cfg.MaxWriteBufferSize,
	}

	for i := 0; i < cfg.Par; i++ {
		pipeImpl := pipe.NewPipe(pipeCfg)
		go pipeImpl.Start()
		time.Sleep(17 * time.Microsecond)
		go pipeImpl.StartTimer()

		c.pool <- pipeImpl
	}

	return c
}

func (c *Client) CallFunc(f func(conn *pipe.Pipe) chan pipe.Res) ([]byte, error) {
	conn := <-c.pool
	ch := f(conn)
	c.pool <- conn
	var res pipe.Res
	select {
	case res = <-ch:
	case <-time.After(3 * time.Second):
		return nil, fmt.Errorf("timeout")
	}
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
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Ping()
	})

	if err != nil || res[0] != constants.Ping {
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

func (c *Client) CreateSpace(engineType constants.SpaceEngineType, size uint16, name []byte) result.Result[uint16] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.CreateSpace(engineType, size, name)
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
	// TODO r
	//conn := <-c.pool
	//ch := conn.CreateSpace(engineType, size, name)
	//c.pool <- conn
	//res := <-ch
	//if res.Err != nil {
	//	return result.Result[uint16]{
	//		Value: 0,
	//		Err:   res.Err,
	//		IsOk:  false,
	//	}
	//}
	//err := result.DefineError(res.Slice[0])
	//if err != nil {
	//	return result.Result[uint16]{
	//		Value: 0,
	//		Err:   err,
	//		IsOk:  false,
	//	}
	//}
	//return result.Result[uint16] {
	//	Value: fastbytes.B2U16(res.Slice[1:3]),
	//	Err:   nil,
	//	IsOk:  true,
	//}
}

func (c *Client) GetSpacesNames() result.Result[[]string] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.GetSpacesNames()
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

func (c *Client) Insert(key []byte, value []byte, spaceId uint16) result.Result[result.Void] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Insert(key, value, spaceId)
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

func (c *Client) Set(key []byte, value []byte, spaceId uint16) result.Result[result.Void] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Set(key, value, spaceId)
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

func (c *Client) Get(key []byte, spaceId uint16) result.Result[[]byte] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Get(key, spaceId)
	})
	if err != nil || res[0] != constants.Done {
		return result.Result[[]byte]{
			Value: nil,
			Err:   err,
			IsOk:  false,
		}
	}

	return result.Result[[]byte]{
		Value: res[2:],
		Err:   nil,
		IsOk:  true,
	}
}

func (c *Client) Delete(key []byte, spaceId uint16) result.Result[result.Void] {
	res, err := c.CallFunc(func(conn *pipe.Pipe) chan pipe.Res {
		return conn.Delete(key, spaceId)
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

//// CreateSpace creates a new space in the database. CreateSpace returns the ID of the space.
//func (c *Client) CreateSpace(engineType constants.SpaceEngineType, name string, size uint32) result.Result[uint16] {
//	conn := c.pool.Get().(*conn.Connection)
//	defer c.pool.Put(conn)
//	return conn.CreateSpace(engineType, name, size)
//}
//
//// GetSpacesNames returns the names of all spaces in the database.
//func (c *Client) GetSpacesNames() result.Result[[]string] {
//	conn := c.pool.Get().(*conn.Connection)
//	defer c.pool.Put(conn)
//	return conn.GetSpacesNames()
//}
//
//// Get returns the value of the key. The value is raw bytes.
//func (c *Client) Get(key []byte, spaceId uint16) result.Result[[]byte] {
//	conn := c.pool.Get().(*conn.Connection)
//	defer c.pool.Put(conn)
//	return conn.Get(key, spaceId)
//}
//
//// Set sets a value by a key. It will return void value, check Result.IsOk.
//func (c *Client) Set(key []byte, value []byte, spaceId uint16) result.Result[result.Void] {
//	conn := c.pool.Get().(*conn.Connection)
//	defer c.pool.Put(conn)
//	return conn.Set(key, value, spaceId)
//}
