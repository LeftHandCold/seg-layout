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
	for i := 0; i < 512; i++ {
		seg.Append(file, []byte(fmt.Sprintf("this is tests %d", i)))
	}

	seg.Free(file, 3)
	seg.Free(file, 40)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 512)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
}
