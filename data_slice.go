/*
*	Copyright (c) 2023
*	John's Page All rights reserved.
*
*	Redistribution and use in source and binary forms, with or without
*	modification, are permitted provided that the following conditions
*	are met:
*
*	Redistributions of source code must retain the above copyright notice,
*	this list of conditions and the following disclaimer.
*
*	THIS SOFTWARE IS PROVIDED BY [Name of Organization] “AS IS” AND ANY EXPRESS
*	OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
*	OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
*	EVENT SHALL [Name of Organisation] BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
*	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
*	PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
*	OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
*	IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
*	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
*	OF SUCH DAMAGE.
 */

package gollowdb

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
