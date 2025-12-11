package main

import (
	"fmt"
	"os"

	"github.com/delta-io/delta-kernel-go/delta"
)

// FilePrinter implements delta.FileVisitor to print file information
type FilePrinter struct {
	fileCount int
}

func (fp *FilePrinter) VisitFile(path string, size int64, stats *delta.Stats, partitionValues map[string]string) {
	fp.fileCount++
	fmt.Printf("  File #%d: %s\n", fp.fileCount, path)
	fmt.Printf("    Size: %d bytes\n", size)
	if stats != nil {
		fmt.Printf("    Records: %d\n", stats.NumRecords)
	}
	if len(partitionValues) > 0 {
		fmt.Printf("    Partition values:\n")
		for k, v := range partitionValues {
			fmt.Printf("      %s = %s\n", k, v)
		}
	}
	fmt.Println()
}

// MetadataCollector implements delta.ScanMetadataVisitor to collect scan metadata
type MetadataCollector struct {
	chunkCount  int
	totalFiles  int
}

func (mc *MetadataCollector) VisitScanMetadata(metadata *delta.ScanMetadata) bool {
	mc.chunkCount++
	fmt.Printf("\n=== Scan Metadata Chunk #%d ===\n\n", mc.chunkCount)

	// Visit all files in this chunk
	filePrinter := &FilePrinter{}
	err := metadata.VisitFiles(filePrinter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error visiting files: %v\n", err)
		return false
	}

	mc.totalFiles += filePrinter.fileCount
	fmt.Printf("Files in this chunk: %d\n", filePrinter.fileCount)

	// Don't close metadata here - iterator will handle it
	return true // Continue iteration
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <table_path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s /path/to/delta/table\n", os.Args[0])
		os.Exit(1)
	}

	tablePath := os.Args[1]

	fmt.Printf("Reading Delta table at: %s\n\n", tablePath)

	// Create a snapshot of the table
	snapshot, err := delta.NewSnapshot(tablePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating snapshot: %v\n", err)
		os.Exit(1)
	}
	defer snapshot.Close()

	// Get the version
	version := snapshot.Version()
	fmt.Printf("Table version: %d\n\n", version)

	// Create a scan
	scan, err := snapshot.Scan()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating scan: %v\n", err)
		os.Exit(1)
	}
	defer scan.Close()

	// Get logical schema
	logicalSchema, err := scan.LogicalSchema()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting logical schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Logical Schema:")
	fmt.Print(logicalSchema.String())
	fmt.Println()

	// Create scan metadata iterator
	iter, err := scan.MetadataIterator(snapshot.Engine())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating scan metadata iterator: %v\n", err)
		os.Exit(1)
	}
	defer iter.Close()

	fmt.Println("=== Starting Scan ===")

	// Iterate over scan metadata
	collector := &MetadataCollector{}
	for {
		hasMore, err := iter.Next(collector)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error iterating scan metadata: %v\n", err)
			os.Exit(1)
		}
		if !hasMore {
			break
		}
	}

	fmt.Printf("\n=== Scan Complete ===\n")
	fmt.Printf("Total chunks: %d\n", collector.chunkCount)
	fmt.Printf("Total files: %d\n", collector.totalFiles)
}
