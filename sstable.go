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

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack/v5"
)

type SSTableReader struct {
	EnhancedIterator[*TableRow]
	path         string
	level        uint64
	id           uint64
	metadata     SSTableMetadata
	file         *os.File
	blocksOffset uint64
	index        *SortedList[*IndexRow]
	comparator   Comparator[*DataSlice]
}

func NewSSTableReader(path string, level uint64, id uint64, comparator Comparator[*DataSlice]) (SSTableReader, error) {
	fd, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("l%d_%d.sst", level, id)), os.O_RDWR, 0644)
	if err != nil {
		return SSTableReader{}, err
	}

	readerBuf := bufio.NewReader(fd)
	meta, err := parseSSTableMetadata(readerBuf)
	if err != nil {
		return SSTableReader{}, err
	}

	offset, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return SSTableReader{}, err
	}
	offset = offset - int64(readerBuf.Buffered())

	index := NewSortedList(meta.index, func(a *IndexRow, b *IndexRow) int {
		return comparator(a.key, b.key)
	})

	reader := SSTableReader{
		path:         path,
		level:        level,
		id:           id,
		metadata:     meta,
		file:         fd,
		blocksOffset: uint64(offset),
		comparator:   comparator,
		index:        index,
	}
	reader.base = &reader

	return reader, nil
}

func (i *SSTableReader) GetPath() string {
	return fmt.Sprintf("%s/s_%d_%d.sst", i.path, i.level, i.id)
}

func (i *SSTableReader) GetLevel() uint64 {
	return i.level
}

func (i *SSTableReader) GetID() uint64 {
	return i.id
}

func (i *SSTableReader) GetMetadata() SSTableMetadata {
	return i.metadata
}

// Returns the first value
func (i *SSTableReader) First() **TableRow {
	rows, err := i.GetBlock(0)
	if err != nil {
		panic(err)
	}

	return rows.First()
}

// Returns the last value
func (i *SSTableReader) Last() **TableRow {
	rows, err := i.GetBlock(i.index.GetSize() - 1)
	if err != nil {
		panic(err)
	}

	return rows.Last()
}

// Returns a key-value mapping associated with the least key greater
// than or equal to the given key, or null if there is no such key.
func (i *SSTableReader) Ceiling(k TableRow) *TableRow {
	// get the block index
	blockIdxRow := i.index.Floor(&IndexRow{key: k.key})
	if blockIdxRow == nil {
		return nil
	}
	blockIdx := (*blockIdxRow).idx

	var block *SortedList[*TableRow]
	var err error

	// get first row from next block
	for blockIdx < i.index.GetSize() {
		block, err = i.GetBlock(blockIdx)
		if err != nil {
			panic(err)
		}
		blockIdx++

		row := block.Ceiling(&k)
		if row != nil {
			return *row
		}
	}

	return nil
}

// Returns a key-value mapping associated with the least key
// strictly greater than the given key, or null if there is
// no such key.
func (i *SSTableReader) Higher(k TableRow) *TableRow {
	// get the block index
	blockIdxRow := i.index.Floor(&IndexRow{key: k.key})
	if blockIdxRow == nil {
		return nil
	}
	blockIdx := (*blockIdxRow).idx

	var block *SortedList[*TableRow]
	var err error

	// get first row from next block
	for blockIdx < i.index.GetSize() {
		block, err = i.GetBlock(blockIdx)
		if err != nil {
			panic(err)
		}
		blockIdx++

		row := block.Higher(&k)
		if row != nil {
			return *row
		}
	}

	return nil
}

// Returns a key-value mapping associated with the least key
// strictly greater than the given key, or null if there is
// no such key.
func (i *SSTableReader) Floor(k TableRow) *TableRow {
	// get the block index
	blockIdxRow := i.index.Floor(&IndexRow{key: k.key})
	if blockIdxRow == nil {
		return nil
	}

	blockIdx := (*blockIdxRow).idx

	block, err := i.GetBlock(blockIdx)
	if err != nil {
		panic(err)
	}

	return *block.Floor(&k)
}

// Returns a key-value mapping associated with the greatest key
// strictly less than the given key, or null if there is no such
// key.
func (i *SSTableReader) Lower(k TableRow) *TableRow {
	// get the block index
	blockIdxRow := *i.index.Floor(&IndexRow{key: k.key})
	if blockIdxRow == nil {
		return nil
	}

	blockIdx := blockIdxRow.idx

	for blockIdx >= 0 {
		block, err := i.GetBlock(blockIdx)
		if err != nil {
			panic(err)
		}
		blockIdx--
		row := block.Lower(&k)

		if row != nil {
			return *row
		}
	}

	return nil
}

// Returns a view of the portion of this map whose keys are
// strictly less than fromKey. O(log n)
func (i *SSTableReader) Head(fromKey TableRow, inclusive bool) Iterable[*TableRow] {
	block, err := i.GetBlock(0)
	if err != nil {
		panic(err)
	}

	itr := Iterable[*TableRow]{
		recreaterCallback: func() IteratorBase[*TableRow] {
			return &SSTableIterator{
				currentIterator:   block.GetIterator(),
				currentBlockIndex: 0,
				sstable:           i,
				stopCallback: func(a **TableRow) bool {
					if inclusive {
						return i.comparator((**a).key, fromKey.key) > 0
					}
					return i.comparator((**a).key, fromKey.key) >= 0
				},
			}
		},
	}

	itr.base = itr
	return itr
}

// Returns a view of the portion of this map whose keys are
// greater than or equal to fromKey.
func (i *SSTableReader) Tail(fromKey TableRow, inclusive bool) Iterable[*TableRow] {
	// get the block index
	b := i.index.Floor(&IndexRow{key: fromKey.key})

	var blockIdx int
	if b == nil {
		blockIdx = 0
	} else {
		blockIdx = (*b).idx
	}

	block, err := i.GetBlock(blockIdx)
	if err != nil {
		panic(err)
	}

	itr := block.Tail(&fromKey, inclusive)
	itrReturn := Iterable[*TableRow]{
		recreaterCallback: func() IteratorBase[*TableRow] {
			return &SSTableIterator{
				currentIterator:   itr.GetIterator(),
				currentBlockIndex: blockIdx,
				sstable:           i,
				stopCallback:      func(a **TableRow) bool { return false },
			}
		},
	}

	itrReturn.base = itrReturn
	return itrReturn
}

// Returns a view of the portion of this map whose keys range
// from fromKey, inclusive, to toKey, exclusive.
func (i *SSTableReader) Sub(fromKey *TableRow, toKey *TableRow, fromInclusive bool, toInclusive bool) Iterable[*TableRow] {
	// get the block index
	blockIdxRow := *i.index.Floor(&IndexRow{key: fromKey.key})

	blockIdx := 0
	if blockIdxRow != nil {
		blockIdx = blockIdxRow.idx
	}

	block, err := i.GetBlock(blockIdx)
	if err != nil {
		panic(err)
	}

	itr := block.Tail(fromKey, fromInclusive)
	itrReturn := Iterable[*TableRow]{
		recreaterCallback: func() IteratorBase[*TableRow] {
			return &SSTableIterator{
				currentIterator:   itr.GetIterator(),
				currentBlockIndex: blockIdx,
				sstable:           i,
				stopCallback: func(a **TableRow) bool {
					if toInclusive {
						return i.comparator((**a).key, toKey.key) > 0
					}
					return i.comparator((**a).key, toKey.key) >= 0
				},
			}
		},
	}

	itrReturn.base = itrReturn
	return itrReturn
}

// Returns all the entry matching the value
func (i *SSTableReader) Get(value *TableRow) Iterable[*TableRow] {
	return i.Sub(value, value, true, true)
}

// Checks if a key is in the tables range
// minKey <= key <= maxKey
func (i *SSTableReader) IsInRange(key any) bool {
	k := NewDataSlice(key)
	return i.comparator(i.metadata.minKey, k) <= 0 && i.comparator(i.metadata.maxKey, k) >= 0
}

// EstimateExistance(key any)
func (i *SSTableReader) Size() int {
	return int(i.metadata.count)
}

func (i *SSTableReader) EstimateExistance(key any) bool {
	if !i.IsInRange(key) {
		return false
	}

	return i.metadata.filter.Contains(NewDataSlice(key))
}

// iterates through all the items
func (i *SSTableReader) GetIterator() IteratorBase[*TableRow] {
	block, err := i.GetBlock(0)
	if err != nil {
		panic(err)
	}

	return &SSTableIterator{
		currentIterator:   block.GetIterator(),
		currentBlockIndex: 0,
		sstable:           i,
		stopCallback:      func(a **TableRow) bool { return false },
	}
}

// Close
func (i *SSTableReader) Close() error {
	return i.file.Close()
}

func (i *SSTableReader) GetBlocksForRange(a *DataSlice, b *DataSlice) []int {
	if i.comparator(a, b) == 0 {
		floor := *i.index.Floor(&IndexRow{key: a})
		return []int{
			floor.idx,
		}
	}

	res := i.index.Sub(&IndexRow{key: a}, &IndexRow{key: b}, true, true)
	itr := res.GetIterator()
	var blocks []int
	for itr.MoveNext() {
		blocks = append(blocks, itr.GetCurrent().idx)
	}
	return blocks
}

func (i *SSTableReader) GetBlock(idx int) (*SortedList[*TableRow], error) {
	offset := i.metadata.index[idx].offset
	size := 0

	if idx+1 == len(i.metadata.index) {
		size = int(i.metadata.size) - offset
	} else {
		size = i.metadata.index[idx+1].offset - offset
	}

	// get bytes
	i.file.Seek(int64(offset)+int64(i.blocksOffset), io.SeekStart)
	blockBytes := make([]byte, size)
	_, err := i.file.Read(blockBytes)
	if err != nil {
		return &SortedList[*TableRow]{}, err
	}

	r, err := snappy.Decode([]byte{}, blockBytes)
	if err != nil {
		return &SortedList[*TableRow]{}, err
	}

	reader := bytes.NewReader(r)
	rows := make([]*TableRow, 0)
	for {
		row, err := NewTableRowFrom(reader)
		if err != nil {
			break
		}
		rows = append(rows, row)
	}

	return NewSortedList(rows, func(a *TableRow, b *TableRow) int {
		return i.comparator(a.key, b.key)
	}), nil
}

// print the index and all of the blocks
func (i *SSTableReader) Print() {
	fmt.Println("=== Index: ===")
	itr := i.index.GetIterator()
	for itr.MoveNext() {
		fmt.Println(itr.GetCurrent().key.String())
	}

	fmt.Println("=== Blocks: ===")
	for idx := 0; idx < i.index.GetSize(); idx++ {
		block, _ := i.GetBlock(idx)
		fmt.Println("Block: ", idx)
		itr := block.GetIterator()
		for itr.MoveNext() {
			fmt.Println(itr.GetCurrent().key.String())
		}
	}
}

func parseSSTableMetadata(reader io.Reader) (SSTableMetadata, error) {
	decoder := msgpack.NewDecoder(reader)

	minKeyBytes, err := decoder.DecodeBytes()
	if err != nil {
		return SSTableMetadata{}, err
	}
	minKey := NewDataSlice(minKeyBytes)

	maxKeyBytes, err := decoder.DecodeBytes()
	if err != nil {
		return SSTableMetadata{}, err
	}
	maxKey := NewDataSlice(maxKeyBytes)

	minSnapshotID, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	maxSnapshotID, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	minTimestamp, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	maxTimestamp, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	blockSize, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	blockCount, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	size, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	count, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	idxSize, err := decoder.DecodeArrayLen()
	if err != nil {
		return SSTableMetadata{}, err
	}

	idx := make([]*IndexRow, idxSize/2)
	j := 0
	for i := 0; i < idxSize; i += 2 {
		keyBytes, err := decoder.DecodeBytes()
		if err != nil {
			return SSTableMetadata{}, err
		}
		key := NewDataSlice(keyBytes)

		offset, err := decoder.DecodeInt()
		if err != nil {
			return SSTableMetadata{}, err
		}
		idx[j] = &IndexRow{
			key:    key,
			offset: offset,
			idx:    j,
		}
		j++
	}

	// decode bloomfilter
	hashCount, err := decoder.DecodeInt()
	if err != nil {
		return SSTableMetadata{}, err
	}

	byteArray, err := decoder.DecodeBytes()
	if err != nil {
		return SSTableMetadata{}, err
	}

	return SSTableMetadata{
		minKey:        minKey,
		maxKey:        maxKey,
		minSnapshotID: uint64(minSnapshotID),
		maxSnapshotID: uint64(maxSnapshotID),
		minTimestamp:  uint64(minTimestamp),
		maxTimestamp:  uint64(maxTimestamp),
		blockSize:     uint64(blockSize),
		blockCount:    uint64(blockCount),
		size:          uint64(size),
		count:         uint64(count),
		index:         idx,
		filter: &Bloomfilter{
			hashCount:     hashCount,
			byteArray:     byteArray,
			byteArraySize: len(byteArray),
		},
	}, nil
}

// Iterator for a sstable
type SSTableIterator struct {
	currentIterator   IteratorBase[*TableRow]
	currentBlockIndex int
	sstable           *SSTableReader
	stopCallback      StopCallback[*TableRow]
}

func (i *SSTableIterator) MoveNext() bool {
	if i.currentIterator == nil {
		return false
	}

	if !i.currentIterator.MoveNext() {
		if i.currentBlockIndex+1 < i.sstable.index.GetSize() {
			i.currentBlockIndex++
			newItr, err := i.sstable.GetBlock(i.currentBlockIndex)
			if err != nil {
				panic(err)
			}
			i.currentIterator = newItr.GetIterator()
			i.currentIterator.MoveNext()
			row := i.currentIterator.GetCurrent()
			return !i.stopCallback(&row)
		}
		return false
	}

	row := i.currentIterator.GetCurrent()
	return !i.stopCallback(&row)
}

func (i *SSTableIterator) GetCurrent() *TableRow {
	if i.currentIterator == nil {
		panic("Iterator: No more items left")
	}

	row := i.currentIterator.GetCurrent()
	if i.stopCallback(&row) {
		panic("Iterator: No more items left")
	}

	return row
}
