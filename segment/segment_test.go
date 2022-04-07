package segment

import "testing"

func TestSegment_Init(t *testing.T) {
	seg := Segment{}
	seg.Init()
	seg.Mount()
	file := seg.NewBlockFile("test")
	seg.Append(file, []byte("this is tests"))
	seg.Append(file, []byte("this is test2"))
}
