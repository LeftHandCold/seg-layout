package segment

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"os"
)

const SIZE = 2 * 1024 * 1024 * 1024
const LOG_START = 2 * 4096
const DATA_START = LOG_START + 1024*4096
const DATA_SIZE = SIZE - DATA_START
const LOG_SIZE = DATA_START - LOG_START

type SuperBlock struct {
	version   uint64
	blockSize uint32
	colCnt    uint32
	lognode   Inode
}

type Segment struct {
	segFile   *os.File
	lastInode uint64
	super     SuperBlock
	nodes     map[string]*BlockFile
	log       *Log
	allocator *BitmapAllocator
}

func (s *Segment) Init() {
	s.super = SuperBlock{
		version:   1,
		blockSize: 4096,
		colCnt:    5,
	}
	log := Inode{
		inode: 1,
		size:  0,
	}

	s.super.lognode = log
	segmentFile, err := os.Create("1.segment")
	s.segFile = segmentFile
	if err != nil {
		return
	}
	err = s.segFile.Truncate(SIZE)

	if err != nil {
		return
	}
	var sbuffer bytes.Buffer
	/*header := make([]byte, 32)
	copy(header, encoding.EncodeUint64(sb.version))*/
	err = binary.Write(&sbuffer, binary.BigEndian, s.super.version)
	if err != nil {
		return
	}
	binary.Write(&sbuffer, binary.BigEndian, uint8(compress.Lz4))
	binary.Write(&sbuffer, binary.BigEndian, s.super.blockSize)
	binary.Write(&sbuffer, binary.BigEndian, s.super.colCnt)

	cbufLen := (s.super.blockSize - (uint32(sbuffer.Len()) % s.super.blockSize)) + uint32(sbuffer.Len())

	if cbufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, cbufLen-uint32(sbuffer.Len()))
		binary.Write(&sbuffer, binary.BigEndian, zero)
	}

	len, err := s.segFile.Write(sbuffer.Bytes())
	logutil.Infof("superblock len is %d", len)
	s.segFile.Sync()
}

func (s *Segment) Mount() {
	s.lastInode = 1
	var seq uint64
	seq = 0
	s.nodes = make(map[string]*BlockFile, 4096)
	ino := Inode{inode: s.super.lognode.inode}
	logFile := &BlockFile{
		snode:   ino,
		name:    "logfile",
		segment: s,
	}
	s.log = &Log{}
	s.log.logFile = logFile
	s.log.offset = LOG_START + s.log.logFile.snode.size
	s.log.seq = seq + 1
	s.nodes[logFile.name] = s.log.logFile
	s.allocator = &BitmapAllocator{
		pageSize: s.GetPageSize(),
	}
	s.log.allocator = &BitmapAllocator{
		pageSize: s.GetPageSize(),
	}

	s.allocator.Init(DATA_SIZE, s.GetPageSize())
	s.log.allocator.Init(LOG_SIZE, s.GetPageSize())
}

func (s *Segment) NewBlockFile(fname string) *BlockFile {
	file := s.nodes[fname]
	var ino Inode
	if file == nil {
		ino = Inode{
			inode:      s.lastInode + 1,
			size:       0,
			extents:    make([]Extent, 0),
			logExtents: Extent{},
		}
	}
	file = &BlockFile{
		snode:   ino,
		name:    fname,
		segment: s,
	}
	s.nodes[file.name] = file
	s.lastInode += 1
	return file
}

func (s *Segment) Append(fd *BlockFile, pl []byte) {
	offset, allocated := s.allocator.Allocate(uint64(len(pl)))
	logutil.Infof("level1 is %x, level0 is %x, offset is %d, allocated is %d",
		s.allocator.level1[0], s.allocator.level0[0], offset, allocated)
	if allocated == 0 {
		panic("no space")
	}
	fd.Append(DATA_START+offset, pl)
	s.log.Append(fd)
}

func (s *Segment) Update(fd *BlockFile, pl []byte, fOffset uint64) {
	offset, allocated := s.allocator.Allocate(uint64(len(pl)))
	free := fd.Update(DATA_START+offset, pl, uint32(fOffset))
	for _, ext := range free {
		s.allocator.Free(ext.offset-DATA_START, ext.length)
	}
	logutil.Infof("updagte level1 is %x, level0 is %x, offset is %d, allocated is %d",
		s.allocator.level1[0], s.allocator.level0[0], s.allocator.lastPos, allocated)
	s.log.Append(fd)

}

func (s *Segment) Free(fd *BlockFile, n uint32) {
	for i, ext := range fd.snode.extents {
		if i == int(n-1) {
			s.allocator.Free(ext.offset-DATA_START, ext.length)
		}
	}
}

func (s *Segment) GetPageSize() uint32 {
	return s.super.blockSize
}
