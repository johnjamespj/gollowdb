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

// A Bloom filter is a space-efficient probabilistic data structure
// can as "possibly in set" or "definitely not in set" question efficiently
type Bloomfilter struct {
	byteArray     []uint8
	byteArraySize int
	hashCount     int
}

// creates a new bloomfilter
func NewBloomfilter(estimatedLength int, hashCount int) *Bloomfilter {
	size := (estimatedLength*15)/8 + 1
	var byteArray = make([]uint8, size)
	return &Bloomfilter{byteArray: byteArray, byteArraySize: size, hashCount: hashCount}
}

// returns the size of the filter
func (x Bloomfilter) Size() int {
	return x.byteArraySize
}

// adds an item to the filter
func (x Bloomfilter) Add(value *DataSlice) {
	hashValue := 0
	for i := 0; i < x.hashCount; i++ {
		hashValue = x.hash(value, i)
		x.byteArray[hashValue/8] |= 1 << (hashValue % 8)
	}
}

// checks if a DataSlice in the filter
func (x Bloomfilter) Contains(value *DataSlice) bool {
	hashValue := 0
	for i := 0; i < x.hashCount; i++ {
		hashValue = x.hash(value, i)

		if (x.byteArray[hashValue/8] & (1 << (hashValue % 8))) == 0 {
			return false
		}
	}
	return true
}

// Calculate hash for a given DataSlice, [index] represents
// the unique hash
func (x Bloomfilter) hash(value *DataSlice, index int) int {
	hash := 0
	for _, element := range value.bin {
		hash = (31*hash*(index+1) + int(element)) % (x.byteArraySize * 8)
	}
	return hash
}
