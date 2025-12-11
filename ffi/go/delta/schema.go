package delta

import "fmt"

// Field represents a field in a Delta Lake schema
type Field struct {
	Name     string
	DataType string
	Nullable bool
	Children []*Field // For nested types (struct, array, map)
}

// Schema represents a Delta Lake table schema
type Schema struct {
	Fields []*Field
}

// String returns a string representation of the schema with tree-like formatting
func (s *Schema) String() string {
	result := ""
	for i, field := range s.Fields {
		isLast := i == len(s.Fields)-1
		result += s.printField(field, 0, 0, isLast)
	}
	return result
}

// printField recursively prints a field with proper tree formatting
func (s *Schema) printField(field *Field, indent int, parentsOnLast int, isLast bool) string {
	result := ""

	// Print indentation with vertical lines
	for j := 0; j < indent; j++ {
		if (indent - parentsOnLast) <= j {
			result += "   "
		} else {
			result += "│  "
		}
	}

	// Print tree connector and field info
	prefix := "├"
	if isLast {
		prefix = "└"
	}
	result += fmt.Sprintf("%s─ %s: %s\n", prefix, field.Name, field.DataType)

	// Recursively print children
	if field.Children != nil {
		newParentsOnLast := parentsOnLast
		if isLast {
			newParentsOnLast++
		}
		for i, child := range field.Children {
			childIsLast := i == len(field.Children)-1
			result += s.printField(child, indent+1, newParentsOnLast, childIsLast)
		}
	}

	return result
}

// Schema returns a placeholder schema
// TODO: Implement proper schema extraction via visitor pattern
func (s *Snapshot) Schema() (*Schema, error) {
	return &Schema{
		Fields: []*Field{
			{Name: "schema_extraction", DataType: "not_yet_implemented", Nullable: true},
		},
	}, nil
}
