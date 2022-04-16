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
	/*var ibuffer bytes.Buffer
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
	b.segment.log.offset += uint64(ibuffer.Len())*/
}

func (b *BlockFile) Update(offset uint64, data []byte, pageOffset uint32) uint32 {
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
	var oldOffset uint32
	for i, _ := range b.snode.extents {
		if b.snode.extents[i].pageOffset == pageOffset {
			oldOffset = b.snode.extents[i].offset
			b.snode.extents[i].offset = uint32(offset)
		}
	}
	logutil.Infof("extents is %d", len(b.snode.extents))
	return oldOffset
}
