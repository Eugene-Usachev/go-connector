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
	"github.com/Eugene-Usachev/go-connector/internal/pipe"
	"log"
)

type Client struct {
	pool chan *pipe.Pipe
}

func NewClient(host, port string, par int) *Client {
	c := &Client{
		pool: make(chan *pipe.Pipe, par),
	}

	for i := 0; i < par; i++ {
		pipeImpl, err := pipe.NewPipe(host, port)
		if err != nil {
			log.Println("[NimbleDB] Error creating pipe: ", err)
		}
		go pipeImpl.Start()
		c.pool <- pipeImpl
	}

	return c
}

// Ping returns true if the connection is alive and false otherwise.
func (c *Client) Ping() bool {
	conn := <-c.pool
	ch := conn.Ping()
	c.pool <- conn
	res := <-ch
	return res.Err == nil
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
