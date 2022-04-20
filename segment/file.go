package segment

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"io"
)

type BlockFile struct {
	snode   Inode
	name    string
	segment *Segment
}

func (b *BlockFile) Append(offset uint64, data []byte) {
	var sbuffer bytes.Buffer
	binary.Write(&sbuffer, binary.BigEndian, data)
	cbufLen := (b.segment.super.blockSize - (uint32(sbuffer.Len()) % b.segment.super.blockSize)) + uint32(sbuffer.Len())
	if cbufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, cbufLen-uint32(sbuffer.Len()))
		binary.Write(&sbuffer, binary.BigEndian, zero)
	}
	_, err := b.segment.segFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		panic("seek is failed")
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		panic("write is failed")
	}
	b.snode.extents = append(b.snode.extents, Extent{
		offset:     uint32(offset),
		length:     cbufLen,
		pageOffset: uint32(b.snode.size) / b.segment.super.blockSize,
		pageNum:    cbufLen / b.segment.super.blockSize,
	})
	logutil.Infof("extents is %d", len(b.snode.extents))
	b.snode.size += uint64(cbufLen)
}

func extentsInsert(extents *[]Extent, idx int, vals []Extent) {
	rear := make([]Extent, 0)
	rear = append(rear, (*extents)[idx:]...)
	front := make([]Extent, 0)
	front = append((*extents)[:idx], vals...)
	*extents = append(front, rear...)
}

func (b *BlockFile) repairExtent(offset, fOffset, length uint32) []Extent {
	num := 0
	for i, extent := range b.snode.extents {
		if fOffset > extent.length {
			fOffset -= extent.length
		} else {
			break
		}
		num = i
	}
	free := make([]Extent, 1)
	ext := b.snode.extents[num]
	remaining := ext.length - fOffset
	if fOffset == 0 && remaining == length {
		oldOff := b.snode.extents[num].offset
		b.snode.extents[num].offset = offset
		free = append(free, Extent{
			offset: oldOff,
			length: remaining,
		})
		return free
	}

	b.snode.extents[num].length = fOffset
	vals := make([]Extent, 1)
	free[0].offset = ext.End() - fOffset
	vals[0].offset = offset
	vals[0].length = length
	if remaining > length {
		free[0].length = length
		vals = append(vals, Extent{
			offset: ext.End() - fOffset - length,
			length: remaining - length,
		})
	} else if remaining < length {
		part := length - remaining
		next := b.snode.extents[num+1]
		free[0].length = ext.length - fOffset
		free = append(free, Extent{
			offset: next.offset,
			length: part,
		})
		vals = append(vals, Extent{
			offset: next.offset - part,
			length: next.length - part,
		})
	}
	extentsInsert(&b.snode.extents, num, vals)
	return free
}

func (b *BlockFile) Update(offset uint64, data []byte, fOffset uint32) []Extent {
	var sbuffer bytes.Buffer
	binary.Write(&sbuffer, binary.BigEndian, data)
	cbufLen := (b.segment.super.blockSize - (uint32(sbuffer.Len()) % b.segment.super.blockSize)) + uint32(sbuffer.Len())
	if cbufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, cbufLen-uint32(sbuffer.Len()))
		binary.Write(&sbuffer, binary.BigEndian, zero)
	}
	_, err := b.segment.segFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		panic("seek is failed")
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		panic("write is failed")
	}
	logutil.Infof("extents is %d", len(b.snode.extents))
	return b.repairExtent(uint32(offset), fOffset, cbufLen)
}
