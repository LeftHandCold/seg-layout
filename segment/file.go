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
	cbufLen := uint32(p2roundup(uint64(sbuffer.Len()), uint64(b.segment.super.blockSize)))
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
		offset: uint32(offset),
		length: cbufLen,
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
func extentsRemove(extents *[]Extent, vals []Extent) {
	for _, ext := range vals {
		for i, e := range *extents {
			if e == ext {
				*extents = append((*extents)[:i], (*extents)[i+1:]...)
			}
		}
	}
}

func (b *BlockFile) repairExtent(offset, fOffset, length uint32) []Extent {
	num := 0
	for _, extent := range b.snode.extents {
		if fOffset >= extent.length {
			fOffset -= extent.length
		} else {
			break
		}
		num++
	}
	free := make([]Extent, 0)
	remove := make([]Extent, 0)
	ext := b.snode.extents[num]
	var remaining uint32 = 0
	if ext.length > fOffset+length {
		remaining = ext.length - fOffset - length
	}
	oldOff := b.snode.extents[num].offset
	if fOffset == 0 && ext.length-fOffset-length == 0 {
		b.snode.extents[num].offset = offset
		free = append(free, Extent{
			offset: oldOff,
			length: length,
		})
		return free
	}
	vals := make([]Extent, 1)
	vals[0].offset = offset
	vals[0].length = length
	if remaining > 0 {
		b.snode.extents[num].length = fOffset
		vals = append(vals, Extent{
			offset: ext.End() - remaining,
			length: remaining,
		})
	}
	freeLength := length
	idx := num
	for {
		if freeLength == 0 {
			break
		}
		e := &b.snode.extents[idx]
		if idx == num {
			b.snode.extents[num].length = fOffset
			xLen := ext.length - fOffset
			if xLen > freeLength {
				xLen = freeLength
			}
			free = append(free, Extent{
				offset: e.offset + fOffset,
				length: xLen,
			})
			freeLength -= xLen
			if freeLength == 0 {
				break
			}
			idx++
			continue
		}
		xLen := e.length
		if xLen > freeLength {
			xLen = freeLength
			free = append(free, Extent{
				offset: e.offset,
				length: xLen,
			})
			e.offset += xLen
			e.length -= xLen
		} else {
			free = append(free, Extent{
				offset: e.offset,
				length: xLen,
			})
			remove = append(remove, b.snode.extents[idx])
			//b.snode.extents = append(b.snode.extents[:idx], b.snode.extents[idx+1:]...)
		}

		freeLength -= xLen
		idx++
	}
	if len(remove) > 0 {
		extentsRemove(&b.snode.extents, remove)
	}
	extentsInsert(&b.snode.extents, num+1, vals)
	return free
}

func (b *BlockFile) Update(offset uint64, data []byte, fOffset uint32) []Extent {
	var sbuffer bytes.Buffer
	binary.Write(&sbuffer, binary.BigEndian, data)
	cbufLen := uint32(p2roundup(uint64(sbuffer.Len()), uint64(b.segment.super.blockSize)))
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
