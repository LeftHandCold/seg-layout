package segment

type Extent struct {
	offset uint32
	length uint32
}

func (ex Extent) End() uint32 {
	return ex.offset + ex.length
}
