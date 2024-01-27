package constants

type TableEngineType uint8

const (
	Cache    TableEngineType = 0
	InMemory TableEngineType = 1
	OnDisk   TableEngineType = 2
)
