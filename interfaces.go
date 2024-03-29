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

type Comparator[V any] func(a V, b V) int

// Sorted list/map that can be navigated easily
type NavigableList[V any] interface {
	// Returns as a list
	ToList() []V

	// Returns the first value
	First() *V

	// Returns the last value
	Last() *V

	// Returns a key-value mapping associated with the least key greater
	// than or equal to the given key, or null if there is no such key.
	Ceiling(k V) *V

	// Returns a key-value mapping associated with the least key
	// strictly greater than the given key, or null if there is
	// no such key.
	Higher(k V) *V

	// Returns a key-value mapping associated with the greatest key less
	// than or equal to the given key, or null if there is no such key.
	Floor(k V) *V

	// Returns a key-value mapping associated with the greatest key
	// strictly less than the given key, or null if there is no such
	// key.
	Lower(k V) *V

	// Returns a view of the portion of this map whose keys are
	// strictly less than toKey.
	Tail(fromKey V, inclusive bool) Iterable[V]

	// Returns a view of the portion of this map whose keys are
	// greater than or equal to fromKey.
	Head(toKey V, inclusive bool) Iterable[V]

	// Returns a view of the portion of this map whose keys range
	// from fromKey, inclusive, to toKey, exclusive.
	Sub(fromKey V, toKey V, fromInclusive bool, toInclusive bool) Iterable[V]

	// Returns all the entry matching the value
	Get(value V) Iterable[V]

	// iterates through all the items
	GetIterator() IteratorBase[V]
}

// All data store manager struct should implement
// these
type AbstractTable interface {
	NavigableList[TableRow]

	// Checks if a key is in the tables range
	// minKey <= key <= maxKey
	IsInRange(key any) bool

	// checks if an item is in the table
	// usually uses memtable
	EstimateExistance(key any) bool

	// returns the size of the table
	Size() int
}

// In memory memtable/buffer
type Memtable interface {
	AbstractTable

	// Deletes an item from the table
	Delete(key any)

	// updates/adds something to the table
	Put(key any, value any)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
