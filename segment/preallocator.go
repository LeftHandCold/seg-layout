package segment

import (
	"fmt"
	"sync"
	"time"
)

// PreallocConfig represents the configuration for pre-allocation
type PreallocConfig struct {
	InitialSize   uint64        // Initial size to pre-allocate
	GrowthFactor  float64       // Factor to grow pre-allocated space
	MaxSize       uint64        // Maximum size to pre-allocate
	CheckInterval time.Duration // Interval to check and grow pre-allocated space
	MinFreeSpace  uint64        // Minimum free space to maintain
}

// Preallocator manages pre-allocated space
type Preallocator struct {
	config     PreallocConfig
	allocator  *BitmapAllocator
	prealloced []struct {
		offset uint64
		size   uint64
	}
	mutex    sync.RWMutex
	stopChan chan struct{}
}

// NewPreallocator creates a new pre-allocator
func NewPreallocator(allocator *BitmapAllocator, config PreallocConfig) *Preallocator {
	prealloc := &Preallocator{
		config:    config,
		allocator: allocator,
		stopChan:  make(chan struct{}),
	}

	// Initial pre-allocation
	prealloc.preallocate(config.InitialSize)

	// Start background goroutine for space management
	go prealloc.manage()

	return prealloc
}

// preallocate pre-allocates space of the specified size
func (p *Preallocator) preallocate(size uint64) {
	// Allocate space
	result := p.allocator.Allocate(uint32(size))
	if !result.Success {
		fmt.Sprintf("fiale")
		return
	}
	fmt.Sprintf("success")
	// Add to pre-allocated blocks
	p.prealloced = append(p.prealloced, struct {
		offset uint64
		size   uint64
	}{
		offset: result.Offset,
		size:   result.Size,
	})
}

// GetSpace returns a pre-allocated space block
func (p *Preallocator) GetSpace(size uint64) (uint64, uint64, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.preallocate(size)
	// Find a suitable pre-allocated block
	for i, block := range p.prealloced {
		if block.size >= size {
			// Remove the block from pre-allocated list
			p.prealloced = append(p.prealloced[:i], p.prealloced[i+1:]...)
			return block.offset, block.size, true
		}
	}

	return 0, 0, false
}

// ReturnSpace returns a space block to the pre-allocator
func (p *Preallocator) ReturnSpace(offset, size uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.prealloced = append(p.prealloced, struct {
		offset uint64
		size   uint64
	}{
		offset: offset,
		size:   size,
	})
}

// manage handles background tasks for the pre-allocator
func (p *Preallocator) manage() {
	ticker := time.NewTicker(p.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkAndGrow()
		case <-p.stopChan:
			return
		}
	}
}

// checkAndGrow checks available space and grows pre-allocated space if needed
func (p *Preallocator) checkAndGrow() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Calculate total pre-allocated space
	var totalPrealloced uint64
	for _, block := range p.prealloced {
		totalPrealloced += block.size
	}

	// Calculate current utilization
	utilization := p.allocator.GetUtilization()

	// Determine if we need to grow
	if utilization > 0.8 && totalPrealloced < p.config.MaxSize {
		// Calculate new size based on growth factor
		newSize := uint64(float64(totalPrealloced) * p.config.GrowthFactor)
		if newSize > p.config.MaxSize {
			newSize = p.config.MaxSize
		}

		// Pre-allocate additional space
		additionalSize := newSize - totalPrealloced
		if additionalSize > 0 {
			p.preallocate(additionalSize)
		}
	}

	// Check if we have too much pre-allocated space
	if totalPrealloced > p.config.MaxSize {
		// Free excess space
		excess := totalPrealloced - p.config.MaxSize
		for i := len(p.prealloced) - 1; i >= 0; i-- {
			if excess <= 0 {
				break
			}
			block := p.prealloced[i]
			if block.size <= excess {
				p.allocator.Free(uint32(block.offset), uint32(block.size))
				excess -= block.size
				p.prealloced = append(p.prealloced[:i], p.prealloced[i+1:]...)
			}
		}
	}
}

// Close stops the pre-allocator and frees all pre-allocated space
func (p *Preallocator) Close() {
	close(p.stopChan)

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Free all pre-allocated space
	for _, block := range p.prealloced {
		p.allocator.Free(uint32(block.offset), uint32(block.size))
	}
	p.prealloced = nil
}
