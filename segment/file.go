package segment

import (
	"bytes"
)

type BlockFile struct {
	snode Inode
	name  string
}
type FileWriter struct {
	file   BlockFile
	offset uint64
	length uint64
	opType uint32
	buffer bytes.Buffer
}
type FileReader struct {
	file   BlockFile
	offset uint64
	length uint64
	buffer bytes.Buffer
}
