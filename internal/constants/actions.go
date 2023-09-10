package constants

const (
	Done          uint8 = 0
	BadRequest    uint8 = 1
	InternalError uint8 = 2
	SpaceNotFound uint8 = 3
	NotFound      uint8 = 4

	CreateSpace    uint8 = 5
	GetSpacesNames uint8 = 6

	Ping   uint8 = 7
	Get    uint8 = 8
	Insert uint8 = 9
	Set    uint8 = 10
	Delete uint8 = 11
	// BigAction If the number of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
	BigAction uint8 = 255
)
