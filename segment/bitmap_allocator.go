package segment

import "github.com/matrixorigin/matrixone/pkg/logutil"

const UNIT_BYTES = 8 // Length of uint64 bytes
const UNITS_PER_UNITSET = 8
const UNITSET_BYTES = UNIT_BYTES * UNITS_PER_UNITSET
const BITS_PER_UNIT = UNIT_BYTES * 8
const BITS_PER_UNITSET = UNITSET_BYTES * 8
const ALL_UNIT_SET = 0xffffffffffffffff
const ALL_UNIT_CLEAR = 0

type BitmapAllocator struct {
	pageSize	uint32
	level0		[]uint64
	level1 		[]uint64
	available	uint64
	lastPos 	uint64
}

func p2align (x uint64, align uint64) uint64{
	return x & -align
}

func p2roundup (x uint64, align uint64) uint64 {
	return -(-x & -align)
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
	b.lastPos = 0
}

func (b *BitmapAllocator) markAllocL0(start, length uint64)  {
	pos := start
	var bit uint64 = 1 << (start % BITS_PER_UNIT)
	val := &(b.level0[pos / BITS_PER_UNIT])
	for {
		if pos == length {
			break
		}
		*val &= ^bit
		bit <<= 1
		pos++
	}
}

func (b *BitmapAllocator) markAllocL1(start, length uint64)  {
	pos := start / BITS_PER_UNIT
	//end := length / BITS_PER_UNIT
	l1pos := start / BITS_PER_UNITSET
	pos++
	pos = p2roundup(pos, UNITS_PER_UNITSET)

	if (pos % UNITS_PER_UNITSET) == 0 {
		val := &(b.level1[l1pos / BITS_PER_UNIT])
		var bit uint64 = 1 << (l1pos % BITS_PER_UNIT)
		*val &= ^bit
	}
}

func (b *BitmapAllocator) Allocate (len uint64, inode *Inode) (uint64, uint64){
	length := p2roundup(len, uint64(b.pageSize))
	var allocated uint64 = 0
	l1Unitpos := b.lastPos / uint64(b.pageSize)/ BITS_PER_UNITSET / BITS_PER_UNIT
	l1Unitend := cap(b.level1)
	pos := b.lastPos / uint64(b.pageSize)
	for ; length > allocated &&  l1Unitpos < uint64(l1Unitend); l1Unitpos++{
		l1bit := b.level1[l1Unitpos]
		if l1bit == ALL_UNIT_CLEAR {
			b.lastPos += BITS_PER_UNIT
			continue
		}
		toAlloc := length - allocated
		allocated += toAlloc
		l0start := pos * BITS_PER_UNIT
		l0end	:= pos * BITS_PER_UNIT + length / uint64(b.pageSize)
		b.markAllocL0(l0start, l0end)
		l0start = p2align(l0start, BITS_PER_UNITSET)
		l0end = p2roundup(l0end, BITS_PER_UNITSET)
		b.markAllocL1(l0start, l0end)
	}
	offset := b.lastPos
	b.lastPos += allocated
	logutil.Infof("level1 is %x, level0 is %x", b.level1[0], b.level0[0])
	return offset, allocated
}