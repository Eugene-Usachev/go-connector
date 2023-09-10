package result

type Void struct{}

type Result[T any] struct {
	Value T
	Err   error
	IsOk  bool
}
