package result

import (
	"errors"
	"github.com/Eugene-Usachev/go-connector/internal/constants"
)

var (
	BadRequest    = errors.New("bad request")
	InternalError = errors.New("internal error")
	TableNotFound = errors.New("table not found")
	NotFound      = errors.New("not found")
)

func DefineError(code uint8) error {
	switch code {
	case 0:
		return nil
	case constants.BadRequest:
		return BadRequest
	case constants.InternalError:
		return InternalError
	case constants.TableNotFound:
		return TableNotFound
	case constants.NotFound:
		return NotFound
	}
	return errors.New("unknown error")
}
