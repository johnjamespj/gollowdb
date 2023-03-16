package structures

// Container that holds the data
type DataSlice struct {
	bin []byte
}

// Creates a new DataSlice
func NewDataSlice(value any) *DataSlice {
	// check if the value is a string
	if str, ok := value.(string); ok {
		return &DataSlice{bin: []byte(str)}
	}

	// check if the value is a byte array
	if bin, ok := value.([]byte); ok {
		return &DataSlice{bin: bin}
	}

	// check if the value is a DataSlice
	if bin, ok := value.(DataSlice); ok {
		return &bin
	}

	panic("invalid type")
}

// returns the binary array of the data inside
func (dataSlice DataSlice) GetByte() []byte {
	return dataSlice.bin
}

// get the size of the object
func (dataSlice DataSlice) GetSize() int {
	return len(dataSlice.bin)
}

// to string
func (dataSlice DataSlice) String() string {
	return string(dataSlice.bin)
}
