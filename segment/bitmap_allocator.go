package segment

type BitmapAllocator struct {
	Allocate func(int32) error
}
