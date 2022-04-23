package segment

type ExtentType uint8

const (
	APPEND ExtentType = iota
	UPDATE ExtentType = iota
)

type Extent struct {
	typ    ExtentType
	offset uint32
	length uint32
}

func (ex Extent) End() uint32 {
	return ex.offset + ex.length
}
