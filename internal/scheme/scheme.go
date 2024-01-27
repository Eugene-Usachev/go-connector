package scheme

type Scheme struct {
	SizedFields   map[string]SizedFieldType   `json:"sized_fields"`
	UnsizedFields map[string]UnsizedFieldType `json:"unsized_fields"`
}

type SizedFieldType string

const (
	Byte    SizedFieldType = "Byte"
	Bool    SizedFieldType = "Bool"
	Uint8   SizedFieldType = "Uint8"
	Uint16  SizedFieldType = "Uint16"
	Uint32  SizedFieldType = "Uint32"
	Uint64  SizedFieldType = "Uint64"
	Uint128 SizedFieldType = "Uint128"
	Int8    SizedFieldType = "Int8"
	Int16   SizedFieldType = "Int16"
	Int32   SizedFieldType = "Int32"
	Int64   SizedFieldType = "Int64"
	Int128  SizedFieldType = "Int128"
	Float32 SizedFieldType = "Float32"
	Float64 SizedFieldType = "Float64"
)

type UnsizedFieldType string

const (
	String    UnsizedFieldType = "String"
	ByteSlice UnsizedFieldType = "ByteSlice"
)
