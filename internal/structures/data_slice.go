package structures

// Container that holds the data
type DataSlice struct {
	bin []byte
}

// Creates a new DataSlice
func NewDataSlice(value string) DataSlice {
	bin := []byte(value)
	return DataSlice{bin: bin}
}

// returns the binary array of the data inside
func (dataSlice DataSlice) GetByte() []byte {
	return dataSlice.bin
}

// get the size of the object
func (dataSlice DataSlice) GetSize() int {
	return len(dataSlice.bin)
}
