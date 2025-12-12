package delta

import "fmt"

// SchemaBuilder implements SchemaVisitor to build a Schema
type SchemaBuilder struct {
	// Lists of fields, indexed by list ID
	lists []fieldList
}

// fieldList represents a list of fields at a particular level
type fieldList struct {
	fields []*fieldWithChildren
}

// fieldWithChildren tracks a field and its child list ID
type fieldWithChildren struct {
	field       *Field
	childListID int // -1 if no children
}

// NewSchemaBuilder creates a new schema builder
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		lists: make([]fieldList, 0),
	}
}

// MakeFieldList creates a new field list with optional capacity reservation
func (b *SchemaBuilder) MakeFieldList(reserve int) int {
	listID := len(b.lists)
	list := fieldList{
		fields: make([]*fieldWithChildren, 0, reserve),
	}
	b.lists = append(b.lists, list)
	return listID
}

// addField helper to add a field to a sibling list
func (b *SchemaBuilder) addField(siblingListID int, name string, dataType string, nullable bool, childListID int) {
	field := &Field{
		Name:     name,
		DataType: dataType,
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, &fieldWithChildren{
		field:       field,
		childListID: childListID,
	})
}

// VisitStruct is called for struct types
func (b *SchemaBuilder) VisitStruct(siblingListID int, name string, nullable bool, childListID int) {
	b.addField(siblingListID, name, "struct", nullable, childListID)
}

// VisitArray is called for array types
func (b *SchemaBuilder) VisitArray(siblingListID int, name string, nullable bool, childListID int) {
	b.addField(siblingListID, name, "array", nullable, childListID)
}

// VisitMap is called for map types
func (b *SchemaBuilder) VisitMap(siblingListID int, name string, nullable bool, childListID int) {
	b.addField(siblingListID, name, "map", nullable, childListID)
}

// VisitDecimal is called for decimal types with precision and scale
func (b *SchemaBuilder) VisitDecimal(siblingListID int, name string, nullable bool, precision uint8, scale uint8) {
	dataType := fmt.Sprintf("decimal(%d,%d)", precision, scale)
	b.addField(siblingListID, name, dataType, nullable, -1)
}

// Simple type visitor methods
func (b *SchemaBuilder) VisitString(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "string", nullable, -1)
}

func (b *SchemaBuilder) VisitLong(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "long", nullable, -1)
}

func (b *SchemaBuilder) VisitInteger(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "integer", nullable, -1)
}

func (b *SchemaBuilder) VisitShort(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "short", nullable, -1)
}

func (b *SchemaBuilder) VisitByte(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "byte", nullable, -1)
}

func (b *SchemaBuilder) VisitFloat(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "float", nullable, -1)
}

func (b *SchemaBuilder) VisitDouble(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "double", nullable, -1)
}

func (b *SchemaBuilder) VisitBoolean(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "boolean", nullable, -1)
}

func (b *SchemaBuilder) VisitBinary(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "binary", nullable, -1)
}

func (b *SchemaBuilder) VisitDate(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "date", nullable, -1)
}

func (b *SchemaBuilder) VisitTimestamp(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "timestamp", nullable, -1)
}

func (b *SchemaBuilder) VisitTimestampNtz(siblingListID int, name string, nullable bool) {
	b.addField(siblingListID, name, "timestamp_ntz", nullable, -1)
}

// resolveChildren recursively resolves child list references into actual Field.Children
func (b *SchemaBuilder) resolveChildren(listID int) []*Field {
	if listID < 0 || listID >= len(b.lists) {
		return nil
	}

	list := b.lists[listID]
	result := make([]*Field, len(list.fields))

	for i, fwc := range list.fields {
		result[i] = fwc.field
		// Resolve children if this field has a child list
		if fwc.childListID >= 0 {
			result[i].Children = b.resolveChildren(fwc.childListID)
		}
	}

	return result
}

// Build returns the built schema from the root list
func (b *SchemaBuilder) Build(rootListID int) *Schema {
	if rootListID < 0 || rootListID >= len(b.lists) {
		return &Schema{Fields: []*Field{}}
	}

	return &Schema{
		Fields: b.resolveChildren(rootListID),
	}
}
