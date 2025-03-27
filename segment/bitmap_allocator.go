package segment

import (
	"sync"
)

const (
	// Bitmap constants
	unitBytes       = 8                 // Length of uint64 bytes (2^3)
	unitsPerUnitSet = 8                 // 2^3
	unitSetBytes    = unitBytes << 3    // 2^6
	bitsPerUnit     = unitBytes << 3    // 2^6
	bitsPerUnitSet  = unitSetBytes << 3 // 2^9
	allUnitSet      = 0xffffffffffffffff
	allUnitClear    = 0

	// Allocation constants
	blockSize   = 4 << 10 // 4 KiB (2^12)
	maxDiskSize = 1 << 40 // 1 TiB (2^40)
)

// BitmapAllocator manages space allocation using a two-level bitmap
type BitmapAllocator struct {
	level0    []uint64 // Level 0 bitmap for individual blocks
	level1    []uint64 // Level 1 bitmap for unit sets
	totalSize uint64   // Total size of managed space
	pageSize  uint32   // Size of each page
	allocated uint64   // Total allocated space
	mu        sync.RWMutex
}

// NewBitmapAllocator creates a new bitmap allocator
func NewBitmapAllocator() *BitmapAllocator {
	return &BitmapAllocator{
		pageSize: 4096, // Default page size
	}
}

// Init initializes the bitmap allocator with the specified size
func (b *BitmapAllocator) Init(size uint64, pageSize uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.totalSize = size
	b.pageSize = pageSize

	// Calculate number of bits needed for level 0
	numBits := (size + uint64(pageSize) - 1) / uint64(pageSize)
	numWords := (numBits + 63) / 64
	b.level0 = make([]uint64, numWords)

	// Calculate number of bits needed for level 1
	numLevel1Words := (numWords + bitsPerUnitSet - 1) / bitsPerUnitSet
	b.level1 = make([]uint64, numLevel1Words)

	// Initialize all bits to 0 (free)
	for i := range b.level0 {
		b.level0[i] = allUnitClear
	}
	for i := range b.level1 {
		b.level1[i] = allUnitClear
	}

	b.allocated = 0
}

// Allocate allocates space of the specified size
func (b *BitmapAllocator) Allocate(size uint32) *Result {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size == 0 {
		return &Result{Success: false}
	}
	length := bitmapRoundup(uint64(size), uint64(b.pageSize))
	// Calculate number of pages needed
	numPages := (length + uint64(b.pageSize) - 1) / uint64(b.pageSize)

	// Check if we have enough total space
	if numPages*uint64(b.pageSize) > b.totalSize-b.allocated {
		return &Result{Success: false}
	}

	startBit := b.findFreeSpace(numPages)
	if startBit == ^uint64(0) {
		return &Result{Success: false}
	}

	// Verify the allocation is within bounds
	if startBit*uint64(b.pageSize)+length > b.totalSize {
		return &Result{Success: false}
	}

	// Mark space as allocated in both levels
	b.markAllocated(startBit, numPages)
	b.allocated += length

	return &Result{
		Success: true,
		Offset:  startBit * uint64(b.pageSize),
		Size:    length,
	}
}

// Free releases allocated space
func (b *BitmapAllocator) Free(offset, size uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	startBit := uint64(offset) / uint64(b.pageSize)
	numPages := (uint64(size) + uint64(b.pageSize) - 1) / uint64(b.pageSize)
	b.markFree(startBit, numPages)
	if b.allocated >= uint64(size) {
		b.allocated -= uint64(size)
	}
}

// GetUtilization returns the current space utilization
func (b *BitmapAllocator) GetUtilization() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.totalSize == 0 {
		return 0
	}
	return float64(b.allocated) / float64(b.totalSize)
}

// GetTotalAllocated returns the total allocated space
func (b *BitmapAllocator) GetTotalAllocated() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.allocated
}

// GetMemoryUsage returns the memory usage of the allocator
func (b *BitmapAllocator) GetMemoryUsage() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return uint64(len(b.level0)*8 + len(b.level1)*8)
}

func (b *BitmapAllocator) getBitPos(val uint64, start uint32) uint32 {
	var mask uint64 = 1 << start
	for {
		if (start < bitsPerUnit) && (val&mask) == 0 {
			mask <<= 1
			start++
			continue
		}
		break
	}
	return start
}

// findFreeSpace finds a contiguous block of free space using two-level bitmap
func (b *BitmapAllocator) findFreeSpace(numPages uint64) uint64 {
	if len(b.level0) == 0 || len(b.level1) == 0 || numPages == 0 {
		return ^uint64(0)
	}
	// Calculate total available bits
	totalBits := uint64(len(b.level0)) * 64
	if totalBits*uint64(b.pageSize) > b.totalSize {
		totalBits = (b.totalSize + uint64(b.pageSize) - 1) / uint64(b.pageSize)
	}

	var consecutive uint64
	var foundStart uint64

	// First, check level1 to find potential free regions
	for unitSetIdx := uint64(0); unitSetIdx < uint64(len(b.level1)); unitSetIdx++ {
		// Skip if this unit set is fully allocated
		if b.level1[unitSetIdx] == allUnitSet {
			continue
		}

		// Check each bit in the unit set
		for bitIdx := uint64(0); bitIdx < 64; bitIdx++ {
			if b.level1[unitSetIdx]&(uint64(1)<<bitIdx) == 0 {
				// Found a potentially free unit set
				// Calculate the starting bit position in level0
				startBit := (unitSetIdx*64 + bitIdx) * bitsPerUnitSet
				endBit := startBit + bitsPerUnitSet
				if endBit > totalBits {
					endBit = totalBits
				}

				// Check level0 for consecutive free pages
				for bit := startBit; bit < endBit; bit++ {
					wordIdx := bit / 64
					bitPos := bit % 64
					if wordIdx >= uint64(len(b.level0)) {
						break
					}

					if b.level0[wordIdx]&(uint64(1)<<bitPos) == 0 {
						if consecutive == 0 {
							foundStart = bit
						}
						consecutive++
						if consecutive >= numPages {
							return foundStart
						}
					} else {
						consecutive = 0
						foundStart = 0
					}
				}

				// If we found some free pages but not enough, continue searching
				// in the next unit set
				if consecutive > 0 {
					// Check if we can find more free pages in the next unit set
					nextUnitSet := unitSetIdx + 1
					if nextUnitSet < uint64(len(b.level1)) && b.level1[nextUnitSet]&(uint64(1)<<0) == 0 {
						nextStartBit := nextUnitSet * 64 * bitsPerUnitSet
						nextEndBit := nextStartBit + bitsPerUnitSet
						if nextEndBit > totalBits {
							nextEndBit = totalBits
						}

						for bit := nextStartBit; bit < nextEndBit; bit++ {
							wordIdx := bit / 64
							bitPos := bit % 64
							if wordIdx >= uint64(len(b.level0)) {
								break
							}

							if b.level0[wordIdx]&(uint64(1)<<bitPos) == 0 {
								consecutive++
								if consecutive >= numPages {
									return foundStart
								}
							} else {
								break
							}
						}
					}
				}
			} else {
				consecutive = 0
				foundStart = 0
			}
		}
	}

	return ^uint64(0)
}

// bitmapAlign aligns x to the nearest lower multiple of align
func bitmapAlign(x uint64, align uint64) uint64 {
	return x & -align
}

// bitmapRoundup rounds x up to the nearest multiple of align
func bitmapRoundup(x uint64, align uint64) uint64 {
	return -(-x & -align)
}

// markAllocated marks a range of bits as allocated in both levels
func (b *BitmapAllocator) markAllocated(startBit, numPages uint64) {
	// Mark bits in level0
	endBit := startBit + numPages
	for bit := startBit; bit < endBit; bit++ {
		wordIdx := bit / 64
		bitPos := bit % 64
		if wordIdx >= uint64(len(b.level0)) {
			break
		}
		b.level0[wordIdx] |= uint64(1) << bitPos
	}
	startBit = p2align(startBit, bitsPerUnitSet)
	endBit = p2roundup(endBit, bitsPerUnitSet)
	// Update level1 bitmap
	startUnitSet := startBit / bitsPerUnitSet
	endUnitSet := (endBit + bitsPerUnitSet - 1) / bitsPerUnitSet
	for unitSet := startUnitSet; unitSet < endUnitSet; unitSet++ {
		if unitSet >= uint64(len(b.level1))*64 {
			break
		}
		// Check if any bits in the unit set are allocated
		startWord := unitSet * bitsPerUnitSet / 64
		endWord := (unitSet + 1) * bitsPerUnitSet / 64
		if endWord > uint64(len(b.level0)) {
			endWord = uint64(len(b.level0))
		}

		// Check if all bits in the unit set are allocated
		allAllocated := true
		for wordIdx := startWord; wordIdx < endWord; wordIdx++ {
			if b.level0[wordIdx] != allUnitSet {
				allAllocated = false
				break
			}
		}
		// Update level1 bitmap
		level1Idx := unitSet / 64
		level1Bit := unitSet % 64
		if level1Idx < uint64(len(b.level1)) {
			if allAllocated {
				b.level1[level1Idx] |= uint64(1) << level1Bit
			}
		}
	}
}

// markFree marks a range of bits as free in both levels
func (b *BitmapAllocator) markFree(startBit, numPages uint64) {
	// Mark bits in level0
	endBit := startBit + numPages
	for bit := startBit; bit < endBit; bit++ {
		wordIdx := bit / 64
		bitPos := bit % 64
		if wordIdx >= uint64(len(b.level0)) {
			break
		}
		b.level0[wordIdx] &= ^(uint64(1) << bitPos)
	}

	// Update level1 bitmap
	startUnitSet := startBit / bitsPerUnitSet
	endUnitSet := (endBit + bitsPerUnitSet - 1) / bitsPerUnitSet
	for unitSet := startUnitSet; unitSet < endUnitSet; unitSet++ {
		if unitSet >= uint64(len(b.level0))*64/bitsPerUnitSet {
			break
		}
		// Check if any bits in the unit set are allocated
		startWord := unitSet * bitsPerUnitSet / 64
		endWord := (unitSet + 1) * bitsPerUnitSet / 64
		if endWord > uint64(len(b.level0)) {
			endWord = uint64(len(b.level0))
		}

		hasAllocated := false
		for wordIdx := startWord; wordIdx < endWord; wordIdx++ {
			if b.level0[wordIdx] != allUnitClear {
				hasAllocated = true
				break
			}
		}

		level1Idx := unitSet / 64
		level1Bit := unitSet % 64
		if hasAllocated {
			b.level1[level1Idx] |= uint64(1) << level1Bit
		} else {
			b.level1[level1Idx] &= ^(uint64(1) << level1Bit)
		}
	}
}

func p2align(x uint64, align uint64) uint64 {
	return x & -align
}

func p2roundup(x uint64, align uint64) uint64 {
	return -(-x & -align)
}
