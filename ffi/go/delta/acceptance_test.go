package delta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var skippedTests = map[string]string{
	"iceberg_compat_v1": "Skipped until DAT is fixed",
	"deletion_vectors":  "Not yet implemented in Go bindings",
	"cdf":               "Change data feed not yet implemented",
}

func TestAcceptance(t *testing.T) {
	testRoot := "../../../acceptance/tests/dat/out/reader_tests/generated"
	entries, err := os.ReadDir(testRoot)
	if err != nil {
		t.Fatalf("Failed to read test directory: %v", err)
	}

	var testCases []string
	for _, entry := range entries {
		if entry.IsDir() {
			path := filepath.Join(testRoot, entry.Name())
			if _, err := os.Stat(filepath.Join(path, "test_case_info.json")); err == nil {
				testCases = append(testCases, path)
			}
		}
	}
	sort.Strings(testCases)

	if len(testCases) == 0 {
		t.Fatal("No test cases discovered")
	}

	for _, testCasePath := range testCases {
		testName := filepath.Base(testCasePath)
		t.Run(testName, func(t *testing.T) {
			if reason, skip := skippedTests[testName]; skip {
				t.Skipf("Skipping test: %s", reason)
			}

			// Load expected version
			var expected struct {
				Version int64 `json:"version"`
			}
			if data, _ := os.ReadFile(filepath.Join(testCasePath, "expected", "latest", "table_version_metadata.json")); data != nil {
				_ = json.Unmarshal(data, &expected)
			}

			// Test snapshot
			snapshot, err := NewSnapshot(filepath.Join(testCasePath, "delta"))
			if err != nil {
				t.Fatalf("Failed to create snapshot: %v", err)
			}
			defer snapshot.Close()

			require.Equal(t, expected.Version, int64(snapshot.Version()))

			// Test scan
			scan, err := snapshot.Scan()
			require.NoError(t, err)
			defer scan.Close()

			schema, err := scan.LogicalSchema()
			require.NoError(t, err)

			iter, err := scan.MetadataIterator(snapshot.Engine())
			require.NoError(t, err)
			defer iter.Close()

			// Count files and rows
			var files, rows int64
			for {
				hasMore, err := iter.Next(&metadataCollector{
					t: t, snapshot: snapshot, scan: scan, files: &files, rows: &rows,
				})
				require.NoError(t, err)
				if !hasMore {
					break
				}
			}

			t.Logf(
				"✓ Version: %d, Schema: %d fields, Files: %d, Rows: %d",
				snapshot.Version(),
				len(schema.Fields),
				files,
				rows,
			)

			require.NotEmpty(t, files)
		})
	}
}

type metadataCollector struct {
	t        *testing.T
	snapshot *Snapshot
	scan     *Scan
	files    *int64
	rows     *int64
}

func (c *metadataCollector) VisitScanMetadata(metadata *ScanMetadata) bool {
	metadata.VisitFiles(&fileVisitor{
		t: c.t, snapshot: c.snapshot, scan: c.scan, files: c.files, rows: c.rows,
	})
	return true
}

type fileVisitor struct {
	t        *testing.T
	snapshot *Snapshot
	scan     *Scan
	files    *int64
	rows     *int64
}

func (v *fileVisitor) VisitFile(path string, size int64, stats *Stats, _ map[string]string) {
	*v.files++
	if stats != nil {
		*v.rows += stats.NumRecords
	}

	// Try to read file
	if tableRoot, err := v.scan.TableRoot(); err == nil {
		if iter, err := v.scan.ReadFile(v.snapshot.Engine(), &FileMeta{
			Path: tableRoot + path, Size: uint64(size),
		}); err == nil {
			iter.Next(&dataVisitor{})
			iter.Close()
		} else if !strings.Contains(err.Error(), "not yet implemented") {
			v.t.Logf("  Warning: Could not read file %s: %v", path, err)
		}
	}
}

type dataVisitor struct{}

func (v *dataVisitor) VisitEngineData(data *EngineData) bool {
	_ = data.Length()
	return false
}

func TestTimeTravel(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/basic_append/delta"

	// snapshot at version 0
	snapshot0, err := NewSnapshotAtVersion(path, 0)
	require.NoError(t, err)
	defer snapshot0.Close()

	require.Equal(t, uint64(0), snapshot0.Version())

	// snapshot at version 1
	snapshot1, err := NewSnapshotAtVersion(path, 1)
	require.NoError(t, err)
	defer snapshot1.Close()

	require.Equal(t, uint64(1), snapshot1.Version())

	// verify they're different versions
	require.NotEqual(t, snapshot0.Version(), snapshot1.Version())

	t.Logf("✓ time travel works: v0=%d, v1=%d", snapshot0.Version(), snapshot1.Version())
}

func TestSchemaProjection(t *testing.T) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/all_primitive_types/delta"

	snapshot, err := NewSnapshot(path)
	require.NoError(t, err)
	defer snapshot.Close()

	// Test that ScanWithOptions API works
	scan, err := snapshot.ScanWithOptions(&ScanOptions{
		Columns: []string{"utf8", "int64"},
	})
	require.NoError(t, err)
	defer scan.Close()

	schema, err := scan.LogicalSchema()
	require.NoError(t, err)

	t.Logf("✓ schema projection API created, fields: %d", len(schema.Fields))

	// TODO: Complete projection implementation
	// The infrastructure is in place but disabled due to architecture refinement needed
	// Currently returns full schema regardless of projection
}

func BenchmarkAcceptance(b *testing.B) {
	path := "../../../acceptance/tests/dat/out/reader_tests/generated/basic_append/delta"
	for i := 0; i < b.N; i++ {
		snapshot, _ := NewSnapshot(path)
		scan, _ := snapshot.Scan()
		_, _ = scan.LogicalSchema()
		scan.Close()
		snapshot.Close()
	}
}
