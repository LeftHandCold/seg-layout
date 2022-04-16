package segment

import (
	"fmt"
	"testing"
)

func TestSegment_Init(t *testing.T) {
	seg := Segment{}
	seg.Init()
	seg.Mount()
	file := seg.NewBlockFile("test")
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	seg.Update(file, []byte(fmt.Sprintf("this is tests %d", 517)), 0)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 518)))
	/*var sbuffer bytes.Buffer
	binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", 515)))
	var size uint32 = 1024*1024 + 4096
	ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
		binary.Write(&sbuffer, binary.BigEndian, zero)
	}
	seg.Append(file, sbuffer.Bytes())
	for i := 0; i < 256; i++ {
		seg.Append(file, []byte(fmt.Sprintf("this is tests %d", i)))
	}

	seg.Free(file, 1)
	seg.Free(file, 40)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))*/

}
