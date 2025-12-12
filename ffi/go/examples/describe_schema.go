package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/delta-io/delta-kernel-go/delta"
)

func main() {
	tablePath := flag.String("table", "", "Path to Delta table (required)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -table <path>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Describe Delta table schema and metadata\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s -table /path/to/delta/table\n", os.Args[0])
	}
	flag.Parse()

	if *tablePath == "" {
		fmt.Fprintf(os.Stderr, "Error: -table flag is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Opening Delta table at: %s\n\n", *tablePath)

	// Create a snapshot of the table
	snapshot, err := delta.NewSnapshot(*tablePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating snapshot: %v\n", err)
		os.Exit(1)
	}
	defer snapshot.Close()

	// Get the version
	version := snapshot.Version()
	fmt.Printf("✓ Successfully opened table\n")
	fmt.Printf("  Version: %d\n\n", version)

	// Get the table root
	tableRoot, err := snapshot.TableRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting table root: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Table root: %s\n\n", tableRoot)

	// Get partition columns
	partitions, err := snapshot.PartitionColumns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting partition columns: %v\n", err)
		os.Exit(1)
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

	// Create a scan
	fmt.Println("Creating scan...")
	scan, err := snapshot.Scan()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating scan: %v\n", err)
		os.Exit(1)
	}
	defer scan.Close()

	// Get scan table root
	scanTableRoot, err := scan.TableRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting scan table root: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Scan table root: %s\n\n", scanTableRoot)

	// Get logical schema
	logicalSchema, err := scan.LogicalSchema()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting logical schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Logical Schema:")
	fmt.Print(logicalSchema.String())

	// Get physical schema
	physicalSchema, err := scan.PhysicalSchema()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting physical schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nPhysical Schema:")
	fmt.Print(physicalSchema.String())

	fmt.Println("\nNote: Full schema extraction via visitor pattern will be implemented next.")
}
