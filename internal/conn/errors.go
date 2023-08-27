package conn

import "errors"

var (
	WriteError = errors.New("write error")
	ReadError  = errors.New("read error")
)
