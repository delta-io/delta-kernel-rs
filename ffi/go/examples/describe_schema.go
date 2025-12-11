package main

import (
	"fmt"
	"os"

	"github.com/delta-io/delta-kernel-go/delta"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <table_path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s /path/to/delta/table\n", os.Args[0])
		os.Exit(1)
	}

	tablePath := os.Args[1]

	fmt.Printf("Opening Delta table at: %s\n\n", tablePath)

	// Create a snapshot of the table
	snapshot, err := delta.NewSnapshot(tablePath)
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

	// Get and print the schema (placeholder for now)
	schema, err := snapshot.Schema()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Schema:")
	fmt.Print(schema.String())

	fmt.Println("\nNote: Full schema extraction will be implemented next.")
}
