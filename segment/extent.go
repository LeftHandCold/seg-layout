package segment

type Extent struct {
	offset uint64
	length uint32
}

func (ex Extent) End() uint64 {
	return ex.offset + uint64(ex.length)
}
