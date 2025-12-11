package delta

// SchemaBuilder implements SchemaVisitor to build a Schema
type SchemaBuilder struct {
	// Lists of fields, indexed by list ID
	lists []fieldList
}

// fieldList represents a list of fields at a particular level
type fieldList struct {
	fields []*Field
}

// fieldInfo tracks a field being built (may have children)
type fieldInfo struct {
	field      *Field
	childList  int // ID of child list, -1 if no children
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
		fields: make([]*Field, 0, reserve),
	}
	b.lists = append(b.lists, list)
	return listID
}

// VisitStruct is called for struct types
func (b *SchemaBuilder) VisitStruct(siblingListID int, name string, nullable bool, childListID int) {
	field := &Field{
		Name:     name,
		DataType: "struct",
		Nullable: nullable,
	}

	// Store child list ID for later resolution
	// For now, we'll just mark it as struct
	// TODO: Resolve nested structures

	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// VisitString is called for string fields
func (b *SchemaBuilder) VisitString(siblingListID int, name string, nullable bool) {
	field := &Field{
		Name:     name,
		DataType: "string",
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// VisitLong is called for long (int64) fields
func (b *SchemaBuilder) VisitLong(siblingListID int, name string, nullable bool) {
	field := &Field{
		Name:     name,
		DataType: "long",
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// VisitInteger is called for integer (int32) fields
func (b *SchemaBuilder) VisitInteger(siblingListID int, name string, nullable bool) {
	field := &Field{
		Name:     name,
		DataType: "integer",
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// VisitBoolean is called for boolean fields
func (b *SchemaBuilder) VisitBoolean(siblingListID int, name string, nullable bool) {
	field := &Field{
		Name:     name,
		DataType: "boolean",
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// VisitDouble is called for double (float64) fields
func (b *SchemaBuilder) VisitDouble(siblingListID int, name string, nullable bool) {
	field := &Field{
		Name:     name,
		DataType: "double",
		Nullable: nullable,
	}
	b.lists[siblingListID].fields = append(b.lists[siblingListID].fields, field)
}

// Build returns the built schema from the root list
func (b *SchemaBuilder) Build(rootListID int) *Schema {
	if rootListID < 0 || rootListID >= len(b.lists) {
		return &Schema{Fields: []*Field{}}
	}

	return &Schema{
		Fields: b.lists[rootListID].fields,
	}
}
