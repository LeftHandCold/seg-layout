package segment

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Log struct {
	logFile   *BlockFile
	seq       uint64
	offset    uint64
	allocator *BitmapAllocator
}

func (ex Extent) Replay() {

}

func (l Log) Append(file *BlockFile) {
	var ibuffer bytes.Buffer
	segment := l.logFile.segment
	binary.Write(&ibuffer, binary.BigEndian, file.snode.inode)
	binary.Write(&ibuffer, binary.BigEndian, file.snode.size)
	binary.Write(&ibuffer, binary.BigEndian, uint64(len(file.snode.extents)))
	for _, ext := range file.snode.extents {
		binary.Write(&ibuffer, binary.BigEndian, ext.offset)
		binary.Write(&ibuffer, binary.BigEndian, ext.length)
		binary.Write(&ibuffer, binary.BigEndian, ext.pageOffset)
		binary.Write(&ibuffer, binary.BigEndian, ext.pageNum)
	}

	ibufLen := (segment.super.blockSize - (uint32(ibuffer.Len()) % segment.super.blockSize)) + uint32(ibuffer.Len())
	if ibufLen > uint32(ibuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(ibuffer.Len()))
		binary.Write(&ibuffer, binary.BigEndian, zero)
	}
	offset, allocated := l.allocator.Allocate(uint64(ibufLen))
	segment.segFile.Seek(int64(offset+LOG_START), io.SeekStart)
	segment.segFile.Write(ibuffer.Bytes())
	//logutil.Infof("level1 is %x, level0 is %x, offset is %d, allocated is %d, level08 is %x",
	//	l.allocator.level1[0], l.allocator.level0[0], offset, allocated, l.allocator.level0[0])
	l.allocator.Free(file.snode.logExtents.offset, file.snode.logExtents.length)
	file.snode.logExtents.offset = uint32(offset)
	file.snode.logExtents.length = uint32(allocated)
}
