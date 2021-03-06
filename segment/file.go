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
		//panic(any("seek is failed"))
		panic("seek is failed")
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		//panic(any("write is failed"))
		panic("write is failed")
	}
	if len(b.snode.extents) > 0 &&
		b.snode.extents[len(b.snode.extents)-1].End() == uint32(offset) {
		b.snode.extents[len(b.snode.extents)-1].length += cbufLen
	} else {
		b.snode.extents = append(b.snode.extents, Extent{
			typ:    APPEND,
			offset: uint32(offset),
			length: cbufLen,
		})
	}
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
		b.snode.extents[num].typ = UPDATE
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
	vals[0].typ = UPDATE
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
		if freeLength == 0 || idx == len(b.snode.extents) {
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
		//panic(any("seek is failed"))
		panic("seek is failed")
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		//panic(any("write is failed"))
		panic("write is failed")
	}
	logutil.Infof("extents is %d", len(b.snode.extents))
	return b.repairExtent(uint32(offset), fOffset, cbufLen)
}

func (b *BlockFile) GetExtents() *[]Extent {
	return &b.snode.extents
}

func (b *BlockFile) Read(offset, length uint32, data *bytes.Buffer) uint32 {
	remain := uint32(b.snode.size) - offset - length
	num := 0
	for _, extent := range b.snode.extents {
		if offset >= extent.length {
			offset -= extent.length
		} else {
			break
		}
		num++
	}
	var read uint32 = 0
	for {
		if length == 0 || length == remain {
			return read
		}
		buf := data.Bytes()
		readOne := length
		if offset > 0 {
			if b.snode.extents[num].length-offset < length {
				readOne = b.snode.extents[num].length - offset
			}
			offset = 0
		} else if b.snode.extents[num].length < length {
			readOne = b.snode.extents[num].length
		}
		buf = buf[read : read+readOne]
		_, err := b.segment.segFile.Seek(int64(b.snode.extents[num].offset)+int64(offset), io.SeekStart)
		if err != nil {
			//panic(any("seek is failed"))
			panic("seek is failed")
		}
		_, err = b.segment.segFile.Read(buf)
		if err != nil {
			//panic(any("read is failed"))
			panic("read is failed")
		}
		read += readOne
		length -= readOne
		num++
	}
	//binary.Write(&sbuffer, binary.BigEndian, zero)
}
