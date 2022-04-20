package segment

type Extent struct {
	offset     uint32
	length     uint32
	pageOffset uint32
	pageNum    uint32
}

func (ex Extent) End() uint32 {
	return ex.offset + ex.length
}

func (ex Extent) PageEnd() uint32 {
	return ex.pageOffset + ex.pageNum
}
