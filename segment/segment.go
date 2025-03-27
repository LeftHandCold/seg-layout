package segment

import (
	"fmt"
	"sync"
	"time"
)

// Segment represents a memory segment with allocation capabilities
type Segment struct {
	allocator    *BitmapAllocator // Main space allocator
	preallocator *Preallocator    // Pre-allocation manager
	mu           sync.RWMutex     // Read-write mutex for thread safety
}

// NewSegment creates a new segment with the specified size
func NewSegment(size uint64) (*Segment, error) {
	// Create bitmap allocator
	allocator := NewBitmapAllocator()
	allocator.Init(size, 4096)

	// Create preallocator with default configuration
	preallocator := NewPreallocator(allocator, PreallocConfig{
		InitialSize:   1024 * 1024, // 1MB
		GrowthFactor:  1.5,
		MaxSize:       100 * 1024 * 1024, // 100MB
		CheckInterval: 5 * time.Minute,
		MinFreeSpace:  10 * 1024 * 1024, // 10MB
	})

	return &Segment{
		allocator:    allocator,
		preallocator: preallocator,
	}, nil
}

// Allocate allocates space of the specified size
func (s *Segment) Allocate(size uint64) (*Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Try to get pre-allocated space first
	offset, size, found := s.preallocator.GetSpace(size)
	if found {
		return &Result{
			Success: true,
			Offset:  offset,
			Size:    size,
		}, nil
	}

	// If no pre-allocated space available, allocate new space
	result := s.allocator.Allocate(uint32(size))
	if !result.Success {
		return nil, fmt.Errorf("failed to allocate space")
	}
	return result, nil
}

// Free releases allocated space
func (s *Segment) Free(offset, size uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Try to return to pre-allocator first
	s.preallocator.ReturnSpace(offset, size)

	// Also free from the main allocator
	s.allocator.Free(uint32(offset), uint32(size))
	return nil
}

// GetUtilization returns the current space utilization
func (s *Segment) GetUtilization() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.allocator.GetUtilization()
}

// GetTotalAllocated returns the total allocated space
func (s *Segment) GetTotalAllocated() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.allocator.GetTotalAllocated()
}

// GetMemoryUsage returns the memory usage of the segment
func (s *Segment) GetMemoryUsage() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.allocator.GetMemoryUsage()
}

// Close closes the segment and frees all resources
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close preallocator
	s.preallocator.Close()

	// Reset allocator
	s.allocator = nil
	s.preallocator = nil

	return nil
}

// String returns a string representation of the segment
func (s *Segment) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("Segment(utilization=%.2f%%, total_allocated=%d, memory_usage=%d)",
		s.GetUtilization()*100,
		s.GetTotalAllocated(),
		s.GetMemoryUsage())
}
