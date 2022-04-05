package segment

const UNIT_BYTES = 8 // Length of uint64 bytes
const UNITS_PER_UNITSET = 8
const UNITSET_BYTES = UNIT_BYTES * UNITS_PER_UNITSET
const BITS_PER_UNIT = UNIT_BYTES * 8
const BITS_PER_UNITSET = UNITSET_BYTES * 8
const ALL_UNIT_SET = 0xffffffffffffffff
const ALL_UNIT_CLEAR = 0;

type BitmapAllocator struct {
	pageSize	uint32
	level0		[]uint64
	level1 		[]uint64
	available	uint64
}

func p2align (x uint64, align uint64)  uint64{
	return x & -align
}

func (b *BitmapAllocator) Init (capacity uint64, pageSize uint32)  {
	b.pageSize = pageSize
	l0granularity := pageSize
	l1granularity := l0granularity * BITS_PER_UNITSET
	l0UnitCount := capacity / uint64(l0granularity) / BITS_PER_UNIT
	b.level0 = make([]uint64, l0UnitCount)
	for i, _ := range b.level0{
		b.level0[i] = ALL_UNIT_SET
	}

	l1UnitCount := capacity / uint64(l1granularity) / BITS_PER_UNIT
	b.level1 = make([]uint64, l1UnitCount)
	for i, _ := range b.level1{
		b.level1[i] = ALL_UNIT_SET
	}
	b.available = p2align(capacity, uint64(pageSize))
}

func (b *BitmapAllocator) Allocate (len uint64, inode *Inode) {
	
}