package constants

const (
	Done          uint8 = 0
	BadRequest    uint8 = 1
	InternalError uint8 = 2
	TableNotFound uint8 = 3
	NotFound      uint8 = 4

	CreateTableInMemory uint8 = 5
	CreateTableCache    uint8 = 6
	CreateTableOnDisk   uint8 = 7
	GetTablesNames      uint8 = 8

	Ping             uint8 = 9
	GetShardMetadata uint8 = 10
	GetHierarchy     uint8 = 11

	Get                  uint8 = 12
	GetField             uint8 = 13
	GetFields            uint8 = 14
	Insert               uint8 = 15
	Set                  uint8 = 16
	Delete               uint8 = 17
	GetAndResetCacheTime uint8 = 18
	// BigAction If the amount of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
	BigAction uint8 = 255
)
