package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"seg-layout/segment"
)

const (
	TiB            = 1024 * 1024 * 1024 * 1024
	MaxRequestSize = 4 * 1024 * 1024
	MinRequestSize = 512
)

var Debug bool

type TestConfig struct {
	deleteRatio     float64 // Ratio of delete operations
	maxRequestSize  int64   // Maximum request size in bytes
	minRequestSize  int64   // Minimum request size in bytes
	totalOperations int     // Total number of operations to perform
}

type TestResult struct {
	totalSpace      uint64  // Total space available
	usedSpace       uint64  // Space actually used
	diskUtilization float64 // Disk utilization ratio
	totalDataSize   uint64  // Total size of data written
	memoryUsage     uint64  // Memory usage for management
	operations      int     // Number of operations performed
	allocSuccess    int     // Number of successful allocations
	deleteSuccess   int     // Number of successful deletions
}

// generateRequest generates a random request size that is a multiple of 512 bytes
func generateRequest(config TestConfig) int64 {
	size := rand.Int63n(config.maxRequestSize-config.minRequestSize+1) + config.minRequestSize
	return (size + 511) & ^511 // Round up to nearest 512 bytes
}

// runTest executes the allocation test with given configuration
func runTest(config TestConfig) (*TestResult, error) {
	// Initialize segment with 1 TiB space
	seg, err := segment.NewSegment(uint64(TiB))
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %v", err)
	}
	defer seg.Close()

	result := &TestResult{
		totalSpace: uint64(TiB),
	}

	// Keep track of allocated blocks for deletion
	allocations := make(map[uint64]uint64) // map[offset]size

	for i := 0; i < config.totalOperations; i++ {
		if rand.Float64() < config.deleteRatio && len(allocations) > 0 {
			// Delete operation
			// Randomly select an allocation to delete
			var offset uint64
			for off := range allocations {
				offset = off
				break
			}
			size := allocations[offset]

			if err := seg.Free(offset, size); err != nil {
				fmt.Printf("Failed to free space at offset %d: %v\n", offset, err)
			} else {
				delete(allocations, offset)
				result.deleteSuccess++
			}
		} else {
			// Allocate operation
			size := generateRequest(config)
			res, err := seg.Allocate(uint64(size))
			if err != nil {
				fmt.Printf("Failed to allocate %d bytes: %v\n", size, err)
				return result, err
			} else {
				fmt.Printf("Success to allocate %d \n", size)
			}

			if res != nil && res.Success {
				allocations[res.Offset] = res.Size
				result.allocSuccess++
				result.totalDataSize += uint64(size)
			}
		}
		result.operations++
	}

	// Calculate final statistics
	result.usedSpace = seg.GetTotalAllocated()
	result.diskUtilization = seg.GetUtilization()
	result.memoryUsage = seg.GetMemoryUsage()

	return result, nil
}

func main() {
	// Parse command line flags
	deleteRatio := flag.Float64("delete-ratio", 0.3, "Ratio of delete operations (0.0-1.0)")
	maxSize := flag.Int64("max-size", MaxRequestSize, "Maximum request size in bytes")
	minSize := flag.Int64("min-size", MinRequestSize, "Minimum request size in bytes")
	operations := flag.Int("operations", 1000, "Number of operations to perform")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile := flag.String("memprofile", "", "write memory profile to file")
	flag.Parse()

	// Setup logging
	if Debug {
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	}

	// Setup profiling
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Validate inputs
	if *deleteRatio < 0 || *deleteRatio > 1 {
		log.Println("Delete ratio must be between 0 and 1")
		return
	}

	if *maxSize > MaxRequestSize || *maxSize < MinRequestSize {
		log.Printf("Max size must be between %d and %d bytes\n", MinRequestSize, MaxRequestSize)
		return
	}

	if *minSize < MinRequestSize || *minSize > *maxSize {
		log.Printf("Min size must be between %d and %d bytes\n", MinRequestSize, *maxSize)
		return
	}

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Configure test
	config := TestConfig{
		deleteRatio:     *deleteRatio,
		maxRequestSize:  *maxSize,
		minRequestSize:  *minSize,
		totalOperations: *operations,
	}

	// Run test
	log.Println("Starting allocation test...")
	log.Printf("Configuration: Delete Ratio=%.2f, Size Range=%d-%d bytes, Operations=%d\n",
		config.deleteRatio, config.minRequestSize, config.maxRequestSize, config.totalOperations)

	result, err := runTest(config)
	if err != nil {
		log.Printf("Test failed: %v\n", err)
		return
	}

	// Print results
	log.Println("\nTest Results:")
	log.Printf("Total Operations: %d\n", result.operations)
	log.Printf("Successful Allocations: %d\n", result.allocSuccess)
	log.Printf("Successful Deletions: %d\n", result.deleteSuccess)
	log.Printf("Total Data Written: %.2f GiB\n", float64(result.totalDataSize)/(1024*1024*1024))
	log.Printf("Used Space: %.2f GiB\n", float64(result.usedSpace)/(1024*1024*1024))
	log.Printf("Disk Utilization: %.2f%%\n", result.diskUtilization*100)
	log.Printf("Memory Usage: %.2f MiB\n", float64(result.memoryUsage)/(1024*1024))

	// Write memory profile if requested
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
