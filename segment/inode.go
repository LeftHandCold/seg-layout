package segment

type Inode struct {
	inode   uint64
	size    uint64
	extents []Extent
}
