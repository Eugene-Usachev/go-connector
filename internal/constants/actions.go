package constants

const (
	Done          uint8 = 0
	BadRequest    uint8 = 1
	InternalError uint8 = 2
	SpaceNotFound uint8 = 3
	NotFound      uint8 = 4

	CreateSpaceInMemory uint8 = 5
	CreateSpaceCache    uint8 = 6
	CreateSpaceOnDisk   uint8 = 7
	GetSpacesNames      uint8 = 8

	Ping                 uint8 = 9
	Get                  uint8 = 10
	Insert               uint8 = 11
	Set                  uint8 = 12
	Delete               uint8 = 13
	GetAndResetCacheTime uint8 = 14
	// BigAction If the number of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
	BigAction uint8 = 255
)
