package segment

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"io"
	"os"
)

const SIZE = 2 * 1024 * 1024 * 1024
const LOG_START = 2 * 4096
const DATA_START = LOG_START + 1024*4096

type SuperBlock struct {
	version   uint64
	blockSize uint32
	colCnt    uint32
	lognode   Inode
}

type Segment struct {
	segFile     *os.File
	lastInode   uint64
	super       SuperBlock
	nodes       map[string]*BlockFile
	log         Log
	GetPageSize func() int32

	SyncMetadata func()
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
	logFile := BlockFile{
		snode: ino,
		name:  "logfile",
	}
	logWriter := &FileWriter{
		file:   logFile,
		offset: LOG_START + logFile.snode.size,
	}
	s.log.writer = logWriter
	s.log.seq = seq + 1
	s.nodes[logFile.name] = &logFile
}

func (s *Segment) NewBlockFile(fname string) *BlockFile {
	file := s.nodes[fname]
	var ino Inode
	if file == nil {
		ino = Inode{
			inode:   s.lastInode + 1,
			size:    0,
			extents: make([]Extent, 0),
		}
	}
	file = &BlockFile{
		snode: ino,
		name:  fname,
	}
	s.nodes[file.name] = file
	s.lastInode += 1
	return file
}

func (s *Segment) Append(fd *BlockFile, pl []byte) {
	var sbuffer bytes.Buffer
	binary.Write(&sbuffer, binary.BigEndian, pl)
	_, err := s.segFile.Seek(DATA_START, io.SeekStart)
	if err != nil {
		panic("seek is failed")
	}
	_, err = s.segFile.Write(sbuffer.Bytes())
	if err != nil {
		panic("write is failed")
	}
	cbufLen := (s.super.blockSize - (uint32(sbuffer.Len()) % s.super.blockSize)) + uint32(sbuffer.Len())
	fd.snode.extents = append(fd.snode.extents, Extent{
		offset: DATA_START,
		length: cbufLen,
	})
	fd.snode.size += uint64(cbufLen)
	var ibuffer bytes.Buffer
	binary.Write(&ibuffer, binary.BigEndian, fd.snode.inode)
	binary.Write(&ibuffer, binary.BigEndian, fd.snode.size)
	for _, ext := range fd.snode.extents {
		binary.Write(&ibuffer, binary.BigEndian, ext.offset)
		binary.Write(&ibuffer, binary.BigEndian, ext.length)
	}

	ibufLen := (s.super.blockSize - (uint32(ibuffer.Len()) % s.super.blockSize)) + uint32(ibuffer.Len())

	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, cbufLen-uint32(ibuffer.Len()))
		binary.Write(&ibuffer, binary.BigEndian, zero)
	}
	s.segFile.Seek(int64(s.log.writer.offset), io.SeekStart)
	s.segFile.Write(ibuffer.Bytes())
}
