package segment

import (
	"bytes"
	"encoding/binary"
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
	_, err := b.segment.segFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		panic("seek is failed")
	}
	_, err = b.segment.segFile.Write(sbuffer.Bytes())
	if err != nil {
		panic("write is failed")
	}
	cbufLen := (b.segment.super.blockSize - (uint32(sbuffer.Len()) % b.segment.super.blockSize)) + uint32(sbuffer.Len())
	/*b.snode.extents = append(b.snode.extents, Extent{
		offset: offset,
		length: cbufLen,
	})*/
	b.snode.size += uint64(cbufLen)
	var ibuffer bytes.Buffer
	binary.Write(&ibuffer, binary.BigEndian, b.snode.inode)
	binary.Write(&ibuffer, binary.BigEndian, b.snode.size)
	binary.Write(&ibuffer, binary.BigEndian, uint64(len(b.snode.extents)))
	for _, ext := range b.snode.extents {
		binary.Write(&ibuffer, binary.BigEndian, ext.offset)
		binary.Write(&ibuffer, binary.BigEndian, ext.length)
	}

	ibufLen := (b.segment.super.blockSize - (uint32(ibuffer.Len()) % b.segment.super.blockSize)) + uint32(ibuffer.Len())
	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(ibuffer.Len()))
		binary.Write(&ibuffer, binary.BigEndian, zero)
	}
	b.segment.segFile.Seek(int64(b.segment.log.offset), io.SeekStart)
	b.segment.segFile.Write(ibuffer.Bytes())
	b.segment.log.offset += uint64(ibuffer.Len())
}
