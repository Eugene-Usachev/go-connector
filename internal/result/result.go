package result

type Void interface{}

type Result[T any] struct {
	Value T
	Err   error
	IsOk  bool
}
