package delta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectionBasic(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// First, get the full schema to know what fields exist
	fullScan, err := snapshot.Scan()
	require.NoError(t, err)
	defer fullScan.Close()

	fullSchema, err := fullScan.LogicalSchema()
	require.NoError(t, err)

	t.Logf("Full schema has %d fields:", len(fullSchema.Fields))
	for _, field := range fullSchema.Fields {
		t.Logf("  - %s: %s", field.Name, field.DataType)
	}

	// Now test projection with just 2 fields
	projectedScan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: []string{"utf8", "int64"},
	})
	require.NoError(t, err)
	defer projectedScan.Close()

	projectedSchema, err := projectedScan.LogicalSchema()
	require.NoError(t, err)

	t.Logf("\nProjected schema has %d fields:", len(projectedSchema.Fields))
	for _, field := range projectedSchema.Fields {
		t.Logf("  - %s: %s", field.Name, field.DataType)
	}

	// Verify projection worked
	require.Equal(t, 2, len(projectedSchema.Fields), "Should have exactly 2 fields after projection")
	require.Equal(t, "utf8", projectedSchema.Fields[0].Name)
	require.Equal(t, "int64", projectedSchema.Fields[1].Name)
}

func TestProjectionSingleField(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Test projection with single field
	scan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: []string{"utf8"},
	})
	require.NoError(t, err)
	defer scan.Close()

	schema, err := scan.LogicalSchema()
	require.NoError(t, err)

	require.Equal(t, 1, len(schema.Fields))
	require.Equal(t, "utf8", schema.Fields[0].Name)
	require.Equal(t, "string", schema.Fields[0].DataType)
}

func TestProjectionAllFields(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Test projection with multiple safe types (string and integer types)
	// Skip boolean and other complex types for now
	safeFields := []string{"utf8", "int64", "int32", "int16", "int8", "float32", "float64"}

	projectedScan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: safeFields,
	})
	require.NoError(t, err)
	defer projectedScan.Close()

	projectedSchema, err := projectedScan.LogicalSchema()
	require.NoError(t, err)

	require.Equal(t, len(safeFields), len(projectedSchema.Fields))
	for i, fieldName := range safeFields {
		require.Equal(t, fieldName, projectedSchema.Fields[i].Name)
	}
}

func TestProjectionEmptyColumns(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Empty columns list should return full schema
	scan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: []string{},
	})
	require.NoError(t, err)
	defer scan.Close()

	schema, err := scan.LogicalSchema()
	require.NoError(t, err)

	// Should have all fields (12 in all_primitive_types)
	require.Equal(t, 12, len(schema.Fields))
}

func TestProjectionNilOptions(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Nil options should return full schema
	scan, err := snapshot.ScanWithOptions(nil)
	require.NoError(t, err)
	defer scan.Close()

	schema, err := scan.LogicalSchema()
	require.NoError(t, err)

	// Should have all fields
	require.Equal(t, 12, len(schema.Fields))
}

func TestProjectionWithBoolean(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Test that boolean projection now works (was causing SIGSEGV before)
	scan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: []string{"bool", "utf8"},
	})
	require.NoError(t, err)
	defer scan.Close()

	schema, err := scan.LogicalSchema()
	require.NoError(t, err)

	require.Equal(t, 2, len(schema.Fields))
	require.Equal(t, "bool", schema.Fields[0].Name)
	require.Equal(t, "boolean", schema.Fields[0].DataType)
	require.Equal(t, "utf8", schema.Fields[1].Name)
	require.Equal(t, "string", schema.Fields[1].DataType)

	t.Log("✓ Boolean projection works correctly!")
}
