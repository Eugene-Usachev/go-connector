package conn

//import (
//	"github.com/Eugene-Usachev/go-connector/internal/Pipe"
//	"github.com/Eugene-Usachev/go-connector/internal/constants"
//	"log"
//	"net"
//	"sync"
//)
//
//type Connection struct {
//	pipe Pipe.Pipe
//}
//
//const (
//	HOST = "dbms"
//	PORT = "8081"
//	TYPE = "tcp"
//)
//
//const bufferSize = 1024 * 1024
//
//func NewConnection() *Connection {
//	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)
//	if err != nil {
//		log.Fatalf("Can't resolve tcp address: %s", err.Error())
//	}
//	conn, err := net.DialTCP(TYPE, nil, tcpServer)
//	//if err != nil {
//	//	log.Fatalf("Can't connect to the dbms: %s", err.Error())
//	//}
//	return &Connection{
//		pipe:     [bufferSize]byte{},
//		conn:     conn,
//		pipeSize: 0,
//		m:        sync.Mutex{},
//		writeBuf: make([]byte, 0, 64*1024),
//		readBuf:  make([]byte, 0, 64*1024),
//
//		result: make(chan []byte, 1024),
//	}
//}
//
//// Close closes the connection.
//func (c *Connection) Close() {
//	c.conn.Close()
//}
//
//// Ping returns true if the connection is alive and false otherwise.
//func (c *Connection) Ping() bool {
//	var buf = [1]byte{constants.Ping}
//
//	_, err := c.conn.Write(buf[:])
//	if err != nil {
//		return false
//	}
//	l, err := c.conn.Read(buf[:])
//	if err != nil {
//		return false
//	}
//
//	if l != 1 || buf[0] != constants.Ping {
//		return false
//	}
//
//	return true
//}
//
////// CreateSpace creates a new space in the database. CreateSpace returns the ID of the space.
////func (c *Connection) CreateSpace(engineType constants.SpaceEngineType, name string, startSize uint32) result.Result[uint16] {
////	buf := make([]byte, 6, 6+len(name))
////	defer func() {
////		buf = nil
////	}()
////	buf[0] = constants.CreateSpace
////	buf[1] = byte(engineType)
////	buf[2] = uint8(startSize)
////	buf[3] = uint8(startSize >> 8)
////	buf[4] = uint8(startSize >> 16)
////	buf[5] = uint8(startSize >> 24)
////	buf = append(buf, fastbytes.S2B(name)...)
////	_, err := c.conn.Write(buf[:])
////	if err != nil {
////		return result.Result[uint16]{
////			Err:   WriteError,
////			IsOk:  false,
////			Value: 0,
////		}
////	}
////	l, err := c.conn.Read(buf[:3])
////	if err != nil {
////		return result.Result[uint16]{
////			Err:   ReadError,
////			IsOk:  false,
////			Value: 0,
////		}
////	}
////
////	if l != 3 || buf[0] != constants.Done {
////		return result.Result[uint16]{
////			Err:   result.InternalError,
////			IsOk:  false,
////			Value: 0,
////		}
////	}
////
////	return result.Result[uint16]{
////		Err:   nil,
////		IsOk:  true,
////		Value: fastbytes.B2U16(buf[1:l]),
////	}
////}
////
////// GetSpacesNames returns the names of all spaces in the database.
////func (c *Connection) GetSpacesNames() result.Result[[]string] {
////	buf := make([]byte, 16*1024, 16*1024)
////	defer func() {
////		buf = nil
////	}()
////	buf[0] = constants.GetSpacesNames
////	_, err := c.conn.Write(buf[:1])
////	if err != nil {
////		return result.Result[[]string]{
////			Err:   WriteError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////	l, err := c.conn.Read(buf[:])
////	if err != nil {
////		return result.Result[[]string]{
////			Err:   ReadError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if l == 1 {
////		if buf[0] != constants.Done {
////			return result.Result[[]string]{
////				Err:   result.DefineError(buf[0]),
////				IsOk:  false,
////				Value: nil,
////			}
////		} else {
////			return result.Result[[]string]{
////				Err:   nil,
////				IsOk:  true,
////				Value: make([]string, 0),
////			}
////		}
////	}
////
////	res := result.Result[[]string]{
////		Err:   nil,
////		IsOk:  true,
////		Value: make([]string, 0),
////	}
////
////	offset := 1
////
////	for {
////		size := int(fastbytes.B2U16(buf[offset : offset+2]))
////		offset = offset + 2
////		res.Value = append(res.Value, fastbytes.B2S(buf[offset:offset+size]))
////		offset = offset + size
////		if offset == l {
////			break
////		}
////	}
////
////	return res
////}
////
////// Get returns the value of the key. The value is raw bytes.
////func (c *Connection) Get(key []byte, spaceId uint16) result.Result[[]byte] {
////	buf := c.writeBuf
////	rBuf := c.readBuf
////	defer func() {
////		c.writeBuf = buf[:0]
////		if len(rBuf) != 4*1024 {
////			c.readBuf = make([]byte, 4*1024)
////		}
////	}()
////
////	buf = append(buf, []byte{constants.Get, uint8(len(key)), uint8(len(key) >> 8)}...)
////	buf = append(buf, key...)
////	buf = append(buf, []byte{uint8(spaceId), uint8(spaceId >> 8)}...)
////
////	_, err := c.conn.Write(buf[:])
////	if err != nil {
////		return result.Result[[]byte]{
////			Err:   WriteError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	l, err := c.conn.Read(rBuf[:])
////	if err != nil {
////		return result.Result[[]byte]{
////			Err:   ReadError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if l == 0 {
////		return result.Result[[]byte]{
////			Err:   result.InternalError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if l == 1 {
////		return result.Result[[]byte]{
////			Err:   result.DefineError(rBuf[0]),
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if l == 3 {
////		if rBuf[0] == constants.Done {
////			return result.Result[[]byte]{
////				Err:   nil,
////				IsOk:  true,
////				Value: []byte{},
////			}
////		} else {
////			return result.Result[[]byte]{
////				Err:   result.InternalError,
////				IsOk:  false,
////				Value: nil,
////			}
////		}
////	}
////
////	log.Println(string(rBuf[:l]), l)
////
////	return result.Result[[]byte]{
////		Err:   nil,
////		IsOk:  true,
////		Value: rBuf[3:l],
////	}
////}
////
////// Set sets a value by a key. It will return void value, check Result.IsOk.
////func (c *Connection) Set(key []byte, value []byte, spaceId uint16) result.Result[result.Void] {
////	buf := c.writeBuf
////	defer func() {
////		c.writeBuf = buf[:0]
////	}()
////	offset := 0
////	buf = append(buf, []byte{constants.Insert, uint8(len(key)), uint8(len(key) >> 8)}...)
////	buf = append(buf, key...)
////	offset += 3 + len(key)
////	buf = append(buf, []byte{uint8(len(value)), uint8(len(value) >> 8)}...)
////	buf = append(buf, value...)
////	buf = append(buf, []byte{uint8(spaceId), uint8(spaceId >> 8)}...)
////
////	_, err := c.conn.Write(buf[:])
////	if err != nil {
////		return result.Result[result.Void]{
////			Err:   WriteError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////	l, err := c.conn.Read(buf[:])
////	if err != nil {
////		return result.Result[result.Void]{
////			Err:   ReadError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if l != 1 {
////		return result.Result[result.Void]{
////			Err:   result.InternalError,
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	if buf[0] != constants.Done {
////		return result.Result[result.Void]{
////			Err:   result.DefineError(buf[0]),
////			IsOk:  false,
////			Value: nil,
////		}
////	}
////
////	return result.Result[result.Void]{
////		Err:   nil,
////		IsOk:  true,
////		Value: nil,
////	}
////}
