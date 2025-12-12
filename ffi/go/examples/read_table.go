package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/delta-io/delta-kernel-go/delta"
)

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
*/
import "C"

// FilePrinter implements delta.FileVisitor to print file information
type FilePrinter struct {
	fileCount int
	snapshot  *delta.Snapshot
	scan      *delta.Scan
	readData  bool
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

	// If readData flag is set, actually read the file data
	if fp.readData && fp.snapshot != nil && fp.scan != nil {
		fmt.Printf("    Reading data from file...\n")

		// Get table root for constructing full path
		tableRoot, err := fp.scan.TableRoot()
		if err != nil {
			fmt.Printf("    Error getting table root: %v\n", err)
			fmt.Println()
			return
		}

		// Construct full file path (path is already relative to table root)
		fullPath := tableRoot + path

		// Create FileMeta
		fileMeta := &delta.FileMeta{
			Path:         fullPath,
			LastModified: 0, // Not available from scan metadata
			Size:         uint64(size),
		}

		// Create file read iterator using scan.ReadFile
		readIter, err := fp.scan.ReadFile(fp.snapshot.Engine(), fileMeta)
		if err != nil {
			fmt.Printf("    Error creating file read iterator: %v\n", err)
			fmt.Println()
			return
		}
		defer readIter.Close()

		// Read data batches
		batchVisitor := &DataBatchVisitor{
			snapshot:  fp.snapshot,
			printData: true, // Always print data when reading
		}
		for {
			hasMore, err := readIter.Next(batchVisitor)
			if err != nil {
				fmt.Printf("    Error reading data: %v\n", err)
				break
			}
			if !hasMore {
				break
			}
		}

		fmt.Printf("    Total batches read: %d\n", batchVisitor.batchCount)
		fmt.Printf("    Total rows read: %d\n", batchVisitor.totalRows)
	}

	fmt.Println()
}

// DataBatchVisitor implements delta.EngineDataVisitor to process data batches
type DataBatchVisitor struct {
	batchCount int
	totalRows  uint64
	snapshot   *delta.Snapshot // Store snapshot to access engine
	printData  bool
}

func (dbv *DataBatchVisitor) VisitEngineData(data *delta.EngineData) bool {
	dbv.batchCount++
	length := data.Length()
	dbv.totalRows += length

	fmt.Printf("      Batch #%d: %d rows\n", dbv.batchCount, length)

	if dbv.printData && length > 0 && dbv.snapshot != nil {
		// Get Arrow data for manual parsing
		arrowData, err := data.GetArrowData(dbv.snapshot.Engine())
		if err != nil {
			fmt.Printf("      Error getting arrow data: %v\n", err)
			return true
		}

		// Print schema information
		fmt.Printf("      Columns: ")
		numCols := arrowData.NumColumns()
		for i := 0; i < int(numCols); i++ {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s (%s)", arrowData.ColumnName(i), arrowData.ColumnFormat(i))
		}
		fmt.Printf("\n")

		// Print first few rows
		numRows := arrowData.NumRows()
		maxRowsToPrint := int64(5)
		if numRows < maxRowsToPrint {
			maxRowsToPrint = numRows
		}

		if maxRowsToPrint > 0 {
			fmt.Printf("      First %d rows:\n", maxRowsToPrint)
			for row := int64(0); row < maxRowsToPrint; row++ {
				fmt.Printf("        Row %d: ", row)
				for col := 0; col < int(numCols); col++ {
					if col > 0 {
						fmt.Printf(", ")
					}

					colName := arrowData.ColumnName(col)
					value, isValid := arrowData.GetValue(col, row)

					if isValid {
						fmt.Printf("%s=%v", colName, value)
					} else {
						fmt.Printf("%s=null", colName)
					}
				}
				fmt.Printf("\n")
			}
		}
	}

	// Note: data.Close() is not called here because the caller owns the handle
	// The kernel will free it after the callback returns

	return true // Continue iteration
}

// MetadataCollector implements delta.ScanMetadataVisitor to collect scan metadata
type MetadataCollector struct {
	chunkCount int
	totalFiles int
	snapshot   *delta.Snapshot
	scan       *delta.Scan
	readData   bool
}

func (mc *MetadataCollector) VisitScanMetadata(metadata *delta.ScanMetadata) bool {
	mc.chunkCount++
	fmt.Printf("\n=== Scan Metadata Chunk #%d ===\n\n", mc.chunkCount)

	// Visit all files in this chunk
	filePrinter := &FilePrinter{
		snapshot: mc.snapshot,
		scan:     mc.scan,
		readData: mc.readData,
	}
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
	tablePath := flag.String("table", "", "Path to Delta table (required)")
	readData := flag.Bool("read-data", false, "Actually read and print data from files")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -table <path> [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Read and scan Delta table files\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -table /path/to/delta/table\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -table /path/to/delta/table -read-data\n", os.Args[0])
	}
	flag.Parse()

	if *tablePath == "" {
		fmt.Fprintf(os.Stderr, "Error: -table flag is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Reading Delta table at: %s\n", *tablePath)
	if *readData {
		fmt.Printf("Data reading: ENABLED\n")
	}
	fmt.Println()

	// Create a snapshot of the table
	snapshot, err := delta.NewSnapshot(*tablePath)
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
	collector := &MetadataCollector{
		snapshot: snapshot,
		scan:     scan,
		readData: *readData,
	}
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
