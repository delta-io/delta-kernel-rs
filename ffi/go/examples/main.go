package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/delta-io/delta-kernel-go/delta"
	"github.com/olekukonko/tablewriter"
)

/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
*/
import "C"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "describe":
		describeCommand(os.Args[2:])
	case "read":
		readCommand(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command '%s'\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  describe    Describe table schema and metadata\n")
	fmt.Fprintf(os.Stderr, "  read        Read and scan table files\n")
	fmt.Fprintf(os.Stderr, "  help        Show this help message\n")
	fmt.Fprintf(os.Stderr, "\nRun '%s <command> -h' for command-specific help\n", os.Args[0])
}

func describeCommand(args []string) {
	fs := flag.NewFlagSet("describe", flag.ExitOnError)
	tablePath := fs.String("table", "", "Path to Delta table (required)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s describe -table <path>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Describe Delta table schema and metadata\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s describe -table /path/to/delta/table\n", os.Args[0])
	}

	fs.Parse(args)

	if *tablePath == "" {
		fmt.Fprintf(os.Stderr, "Error: -table flag is required\n\n")
		fs.Usage()
		os.Exit(1)
	}

	if err := runDescribe(*tablePath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func readCommand(args []string) {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	tablePath := fs.String("table", "", "Path to Delta table (required)")
	readData := fs.Bool("read-data", false, "Actually read and print data from files")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s read -table <path> [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Read and scan Delta table files\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s read -table /path/to/delta/table\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s read -table /path/to/delta/table -read-data\n", os.Args[0])
	}

	fs.Parse(args)

	if *tablePath == "" {
		fmt.Fprintf(os.Stderr, "Error: -table flag is required\n\n")
		fs.Usage()
		os.Exit(1)
	}

	if err := runRead(*tablePath, *readData); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runDescribe(tablePath string) error {
	fmt.Printf("Opening Delta table at: %s\n\n", tablePath)

	snapshot, err := delta.NewSnapshot(tablePath)
	if err != nil {
		return fmt.Errorf("creating snapshot: %w", err)
	}
	defer snapshot.Close()

	version := snapshot.Version()
	fmt.Printf("✓ Successfully opened table\n")
	fmt.Printf("  Version: %d\n\n", version)

	tableRoot, err := snapshot.TableRoot()
	if err != nil {
		return fmt.Errorf("getting table root: %w", err)
	}
	fmt.Printf("Table root: %s\n\n", tableRoot)

	partitions, err := snapshot.PartitionColumns()
	if err != nil {
		return fmt.Errorf("getting partition columns: %w", err)
	}

	if len(partitions) > 0 {
		fmt.Println("Partition columns:")
		for _, col := range partitions {
			fmt.Printf("  - %s\n", col)
		}
	} else {
		fmt.Println("Table has no partition columns")
	}
	fmt.Println()

	fmt.Println("Creating scan...")
	scan, err := snapshot.Scan()
	if err != nil {
		return fmt.Errorf("creating scan: %w", err)
	}
	defer scan.Close()

	scanTableRoot, err := scan.TableRoot()
	if err != nil {
		return fmt.Errorf("getting scan table root: %w", err)
	}
	fmt.Printf("Scan table root: %s\n\n", scanTableRoot)

	logicalSchema, err := scan.LogicalSchema()
	if err != nil {
		return fmt.Errorf("getting logical schema: %w", err)
	}

	fmt.Println("Logical Schema:")
	fmt.Print(logicalSchema.String())

	physicalSchema, err := scan.PhysicalSchema()
	if err != nil {
		return fmt.Errorf("getting physical schema: %w", err)
	}

	fmt.Println("\nPhysical Schema:")
	fmt.Print(physicalSchema.String())

	return nil
}

func runRead(tablePath string, readData bool) error {
	fmt.Printf("Reading Delta table at: %s\n", tablePath)
	if readData {
		fmt.Printf("Data reading: ENABLED\n")
	}
	fmt.Println()

	snapshot, err := delta.NewSnapshot(tablePath)
	if err != nil {
		return fmt.Errorf("creating snapshot: %w", err)
	}
	defer snapshot.Close()

	version := snapshot.Version()
	fmt.Printf("Table version: %d\n\n", version)

	scan, err := snapshot.Scan()
	if err != nil {
		return fmt.Errorf("creating scan: %w", err)
	}
	defer scan.Close()

	logicalSchema, err := scan.LogicalSchema()
	if err != nil {
		return fmt.Errorf("getting logical schema: %w", err)
	}

	fmt.Println("Logical Schema:")
	fmt.Print(logicalSchema.String())
	fmt.Println()

	iter, err := scan.MetadataIterator(snapshot.Engine())
	if err != nil {
		return fmt.Errorf("creating scan metadata iterator: %w", err)
	}
	defer iter.Close()

	fmt.Println("=== Starting Scan ===")

	collector := &MetadataCollector{
		snapshot: snapshot,
		scan:     scan,
		readData: readData,
	}
	for {
		hasMore, err := iter.Next(collector)
		if err != nil {
			return fmt.Errorf("iterating scan metadata: %w", err)
		}
		if !hasMore {
			break
		}
	}

	fmt.Printf("\n=== Scan Complete ===\n")
	fmt.Printf("Total chunks: %d\n", collector.chunkCount)
	fmt.Printf("Total files: %d\n", collector.totalFiles)

	return nil
}

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

	if fp.readData && fp.snapshot != nil && fp.scan != nil {
		fmt.Printf("    Reading data from file...\n")

		tableRoot, err := fp.scan.TableRoot()
		if err != nil {
			fmt.Printf("    Error getting table root: %v\n", err)
			fmt.Println()
			return
		}

		fullPath := tableRoot + path

		fileMeta := &delta.FileMeta{
			Path:         fullPath,
			LastModified: 0,
			Size:         uint64(size),
		}

		readIter, err := fp.scan.ReadFile(fp.snapshot.Engine(), fileMeta)
		if err != nil {
			fmt.Printf("    Error creating file read iterator: %v\n", err)
			fmt.Println()
			return
		}
		defer readIter.Close()

		batchVisitor := &DataBatchVisitor{
			snapshot:  fp.snapshot,
			printData: true,
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
	snapshot   *delta.Snapshot
	printData  bool
}

func (dbv *DataBatchVisitor) VisitEngineData(data *delta.EngineData) bool {
	dbv.batchCount++
	length := data.Length()
	dbv.totalRows += length

	fmt.Printf("      Batch #%d: %d rows\n", dbv.batchCount, length)

	if dbv.printData && length > 0 && dbv.snapshot != nil {
		arrowData, err := data.GetArrowData(dbv.snapshot.Engine())
		if err != nil {
			fmt.Printf("      Error getting arrow data: %v\n", err)
			return true
		}

		numCols := arrowData.NumColumns()
		numRows := arrowData.NumRows()
		maxRowsToPrint := int64(50)
		if numRows < maxRowsToPrint {
			maxRowsToPrint = numRows
		}

		if maxRowsToPrint > 0 {
			// Create table
			table := tablewriter.NewWriter(os.Stdout)

			// Set table header with column names
			header := make([]any, numCols)
			for i := 0; i < int(numCols); i++ {
				header[i] = arrowData.ColumnName(i)
			}
			table.Header(header...)

			// Add rows to table
			for row := int64(0); row < maxRowsToPrint; row++ {
				rowData := make([]any, numCols)
				for col := 0; col < int(numCols); col++ {
					value, isValid := arrowData.GetValue(col, row)
					if isValid {
						rowData[col] = fmt.Sprintf("%v", value)
					} else {
						rowData[col] = "NULL"
					}
				}
				table.Append(rowData...)
			}

			// Render table
			fmt.Printf("      First %d rows:\n", maxRowsToPrint)
			table.Render()
		}
	}

	return true
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

	return true
}
