package client

import (
	"github.com/Eugene-Usachev/fastbytes"
	"log"
	"sync"
	"time"
)

const par = 128
const n = 3 * 1000 * 1000
const count = n / par

func (c *Client) Test() {
	log.Println("Testing client ping...")

	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < par; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < count; j++ {
				c.Ping()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Println("Testing time: ", elapsed, ", speed:", elapsed.Microseconds()/n, "microseconds per operation")

	//c.CreateSpace(constants.Cache, "test", 1024)

	var keys = make([][]byte, n)
	var values = make([][]byte, n)

	for i := 0; i < n; i++ {
		keys[i] = fastbytes.I2B(i)
	}

	for i := 0; i < n; i++ {
		values[i] = fastbytes.S2B("12345678901234567890")
	}

	log.Println("Testing client set...")
	start = time.Now()
	for i := 0; i < par; i++ {
		wg.Add(1)
		go func(ii int) {
			for j := 0; j < count; j++ {
				//c.Set(keys[count*ii+j], values[count*ii+j], uint16(0))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	elapsed = time.Since(start)
	log.Println("Testing time: ", elapsed, ", speed:", elapsed.Microseconds()/int64(n), "microseconds per operation.")
}
