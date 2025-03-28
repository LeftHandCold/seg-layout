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

// Debug mode flag
var debugMode = flag.Bool("debug", false, "Enable debug mode")

type TestConfig struct {
	deleteRatio     float64 // Ratio of delete operations
	maxRequestSize  int64   // Maximum request size in bytes
	minRequestSize  int64   // Minimum request size in bytes
	totalOperations int     // Total number of operations to perform
	targetWriteSize uint64  // Target total write size for endurance test (in bytes)
}

type TestResult struct {
	totalSpace      uint64        // Total space available
	usedSpace       uint64        // Space actually used
	diskUtilization float64       // Disk utilization ratio
	totalDataSize   uint64        // Total size of data written
	memoryUsage     uint64        // Memory usage for management
	operations      int           // Number of operations performed
	allocSuccess    int           // Number of successful allocations
	deleteSuccess   int           // Number of successful deletions
	duration        time.Duration // Test duration
}

// generateRequest generates a random request size that is a multiple of 512 bytes
func generateRequest(config TestConfig) int64 {
	size := rand.Int63n(config.maxRequestSize-config.minRequestSize+1) + config.minRequestSize
	return (size + 511) & ^511 // Round up to nearest 512 bytes
}

// runTest executes the allocation test with given configuration
func runTest(config TestConfig) (*TestResult, error) {
	startTime := time.Now()
	// Initialize segment with 1 TiB space
	seg, err := segment.NewSegment(uint64(TiB))
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %v", err)
	}
	result := &TestResult{
		totalSpace: uint64(TiB),
	}
	defer func() {
		// Calculate final statistics
		result.usedSpace = seg.GetTotalAllocated()
		result.diskUtilization = seg.GetUtilization()
		result.memoryUsage = seg.GetMemoryUsage()
		result.duration = time.Since(startTime)
		seg.Close()
	}()

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
			}

			if res != nil && res.Success {
				allocations[res.Offset] = res.Size
				result.allocSuccess++
				result.totalDataSize += uint64(size)
			}
		}
		result.operations++
	}

	return result, nil
}

// runEnduranceTest executes the endurance test with given configuration
func runEnduranceTest(config TestConfig) (*TestResult, error) {
	startTime := time.Now()

	// Initialize segment with 1 TiB space
	seg, err := segment.NewSegment(uint64(TiB))
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %v", err)
	}

	result := &TestResult{
		totalSpace: uint64(TiB),
	}
	defer func() {
		// Calculate final statistics
		result.usedSpace = seg.GetTotalAllocated()
		result.diskUtilization = seg.GetUtilization()
		result.memoryUsage = seg.GetMemoryUsage()
		result.duration = time.Since(startTime)
		seg.Close()
	}()
	var totalWritten uint64
	cycleCount := 0
	// Phase 1: Write until full or utilization is high enough
	allocations := make(map[uint64]uint64) // map[offset]size
	// Keep running until we reach the target write size
	for totalWritten < config.targetWriteSize {
		cycleCount++
		log.Printf("Starting cycle %d, total written so far: %.2f TiB\n",
			cycleCount, float64(totalWritten)/float64(TiB))

		writeFailCount := 0
		const maxConsecutiveFails = 1  // Consider disk full after this many consecutive failures
		const targetUtilization = 0.95 // Target disk utilization

		for writeFailCount < maxConsecutiveFails {
			currentUtilization := seg.GetUtilization()
			if currentUtilization >= targetUtilization {
				log.Printf("Reached target utilization: %.2f%%\n", currentUtilization*100)
				break
			}

			size := generateRequest(config)
			res, err := seg.Allocate(uint64(size))
			if err != nil || !res.Success {
				writeFailCount++
				if writeFailCount >= maxConsecutiveFails {
					log.Printf("Write failed %d times consecutively\n", writeFailCount)
				}
				continue
			}
			writeFailCount = 0 // Reset on successful write

			allocations[res.Offset] = res.Size
			result.allocSuccess++
			totalWritten += uint64(size)
			result.totalDataSize += uint64(size)
		}

		// Record disk utilization at "full" state
		fullUtilization := seg.GetUtilization()
		log.Printf("Disk utilization when full: %.2f%%\n", fullUtilization*100)

		// Phase 2: Random deletion (30-50%)
		if len(allocations) > 0 {
			deleteRatio := 0.3 + rand.Float64()*0.2 // Random between 30-50%
			initialAllocCount := len(allocations)
			deleteCount := int(float64(initialAllocCount) * deleteRatio)

			// Convert map keys to slice for random selection
			offsets := make([]uint64, 0, initialAllocCount)
			for offset := range allocations {
				offsets = append(offsets, offset)
			}

			// Randomly delete blocks
			deletedSize := uint64(0)
			actualDeleteCount := 0
			for i := 0; i < deleteCount; i++ {
				if len(offsets) == 0 {
					break
				}
				// Select random index
				idx := rand.Intn(len(offsets))
				offset := offsets[idx]
				size := allocations[offset]

				// Delete the block
				if err := seg.Free(offset, size); err != nil {
					log.Printf("Failed to free space at offset %d: %v\n", offset, err)
					continue
				}

				// Update tracking
				delete(allocations, offset)
				result.deleteSuccess++
				deletedSize += size
				actualDeleteCount++

				// Remove from offsets slice
				offsets[idx] = offsets[len(offsets)-1]
				offsets = offsets[:len(offsets)-1]
			}

			log.Printf("Deleted %.2f%% of blocks (%.2f TiB), count %d, total %d, initialAllocCount %d\n",
				float64(actualDeleteCount)/float64(initialAllocCount)*100,
				float64(deletedSize)/float64(TiB), actualDeleteCount, len(allocations), initialAllocCount)

			result.operations += result.allocSuccess + result.deleteSuccess
		} else {
			log.Printf("No blocks available for deletion\n")
			break
		}
	}
	return result, nil
}

func main() {
	// Parse command line flags
	deleteRatio := flag.Float64("delete-ratio", 0.3, "Ratio of delete operations (0.0-1.0)")
	maxSize := flag.Int64("max-size", MaxRequestSize, "Maximum request size in bytes")
	minSize := flag.Int64("min-size", MinRequestSize, "Minimum request size in bytes")
	operations := flag.Int("operations", 1000, "Number of operations to perform")
	targetWrite := flag.Uint64("target-write", 10*TiB, "Target total write size for endurance test")
	testMode := flag.String("mode", "normal", "Test mode: normal or endurance")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to file")
	memProfile := flag.String("memprofile", "", "write memory profile to file")
	flag.Parse()

	// Setup logging
	if *debugMode {
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

	// Configure test
	config := TestConfig{
		deleteRatio:     *deleteRatio,
		maxRequestSize:  *maxSize,
		minRequestSize:  *minSize,
		totalOperations: *operations,
		targetWriteSize: *targetWrite,
	}

	var result *TestResult
	var err error

	// Run test based on mode
	log.Printf("Starting %s test...\n", *testMode)
	if *testMode == "endurance" {
		result, err = runEnduranceTest(config)
	} else {
		result, err = runTest(config)
	}

	// Print results
	log.Println("\nTest Results:")
	log.Printf("Test Error: %v\n", err)
	log.Printf("Test Duration: %v\n", result.duration)
	log.Printf("Total Operations: %d\n", result.operations)
	log.Printf("Successful Allocations: %d\n", result.allocSuccess)
	log.Printf("Successful Deletions: %d\n", result.deleteSuccess)
	log.Printf("Total Data Written: %.2f TiB\n", float64(result.totalDataSize)/float64(TiB))
	log.Printf("Used Space: %.2f GiB\n", float64(result.usedSpace)/float64(1024*1024*1024))
	log.Printf("Disk Utilization: %.2f%%\n", result.diskUtilization*100)
	log.Printf("Memory Usage: %.2f MiB\n", float64(result.memoryUsage)/float64(1024*1024))

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
