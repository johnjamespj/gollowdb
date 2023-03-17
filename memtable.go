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
	"sync/atomic"
	"time"
)

type SkipListMemtable struct {
	table             *Skiplist[*TableRow]
	filter            *Bloomfilter
	maxKey            *DataSlice
	minKey            *DataSlice
	size              int64
	snapshotIdCounter int64
	comparator        Comparator[*DataSlice]
	wal               WALWriter
}

func NewSkipListMemtable(path string, id int64, comparator Comparator[*DataSlice]) *SkipListMemtable {
	cmp := func(a *TableRow, b *TableRow) int {
		return comparator(a.key, b.key)
	}
	memtable := SkipListMemtable{
		table:             CreateSkiplist(10000, cmp),
		comparator:        comparator,
		size:              0,
		snapshotIdCounter: 0,
		wal:               NewWALWriter(path, id),
		filter:            NewBloomfilter(10000, 9),
		maxKey:            NewDataSlice([]byte{}),
		minKey:            NewDataSlice([]byte{}),
	}
	return &memtable
}

func (i *SkipListMemtable) Put(key any, value any) {
	keyDataSlice := NewDataSlice(key)
	valueDataSlice := NewDataSlice(value)
	row := &TableRow{
		key:        keyDataSlice,
		value:      valueDataSlice,
		timestamp:  uint64(time.Now().UnixNano()),
		snapshotId: uint64(atomic.AddInt64(&i.snapshotIdCounter, 1)),
		rowType:    PUT,
	}

	i.wal.AddTableRow(row)
	i.table.Add(row)
	atomic.AddInt64(&i.size, int64(keyDataSlice.GetSize())+int64(valueDataSlice.GetSize())+10)

	if i.comparator(i.maxKey, keyDataSlice) < 0 {
		i.maxKey = keyDataSlice
	} else if i.comparator(i.minKey, keyDataSlice) > 0 {
		i.minKey = keyDataSlice
	}

	i.filter.Add(keyDataSlice)
}

func (i *SkipListMemtable) Delete(key any) {
	keyDataSlice := NewDataSlice(key)
	row := &TableRow{
		key:        keyDataSlice,
		value:      NewDataSlice([]byte{}),
		timestamp:  uint64(time.Now().UnixNano()),
		snapshotId: uint64(atomic.AddInt64(&i.snapshotIdCounter, 1)),
		rowType:    DELETE,
	}

	i.wal.AddTableRow(row)
	i.table.Add(row)
	atomic.AddInt64(&i.size, int64(keyDataSlice.GetSize())+10)
}

func (i *SkipListMemtable) Get(key any) Iterable[*TableRow] {
	return i.table.Get(&TableRow{key: NewDataSlice(key)})
}

func (i *SkipListMemtable) IsInRange(key any) bool {
	keyDataSlice := NewDataSlice(key)
	return i.comparator(i.minKey, keyDataSlice) >= 0 && i.comparator(i.maxKey, keyDataSlice) <= 0
}

func (i *SkipListMemtable) EstimateExistance(key any) bool {
	if !i.IsInRange(key) {
		return false
	}

	return i.filter.Contains(NewDataSlice(key))
}

func (i *SkipListMemtable) GetSize() int {
	return int(i.size)
}

func (i *SkipListMemtable) ToList() []*TableRow {
	return i.table.ToList()
}
