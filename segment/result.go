package segment

// Result represents the result of an allocation request
type Result struct {
	Success bool   // Whether the allocation was successful
	Offset  uint64 // Starting offset of the allocated space
	Size    uint64 // Size of the allocated space
}
