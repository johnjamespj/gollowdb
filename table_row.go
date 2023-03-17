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
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	PUT    int8 = 0
	DELETE int8 = 1
)

// This represents a row in the table
type TableRow struct {
	key        *DataSlice
	value      *DataSlice
	timestamp  uint64
	snapshotId uint64
	rowType    int8
}

// creates a new TableRow struct
func NewTableRow(key *DataSlice, value *DataSlice, timestamp uint64, snapshotId uint64, rowType int8) *TableRow {
	return &TableRow{
		key:        key,
		value:      value,
		timestamp:  timestamp,
		snapshotId: snapshotId,
		rowType:    rowType,
	}
}

// parses a row from an io writer
func NewTableRowFrom(reader io.Reader) (*TableRow, error) {
	var (
		key        []byte
		timestamp  int
		snapshotId int
		rowType    int
		value      []byte

		keySlice   *DataSlice
		valueSlice *DataSlice

		err error
	)

	decoder := msgpack.NewDecoder(reader)

	key, err = decoder.DecodeBytes()
	if err != nil {
		return &TableRow{}, err
	}

	timestamp, err = decoder.DecodeInt()
	if err != nil {
		return &TableRow{}, err
	}

	snapshotId, err = decoder.DecodeInt()
	if err != nil {
		return &TableRow{}, err
	}

	rowType, err = decoder.DecodeInt()
	if err != nil {
		return &TableRow{}, fmt.Errorf("error parsing rowType")
	}

	value, err = decoder.DecodeBytes()
	if err != nil {
		return &TableRow{}, err
	}

	keySlice = NewDataSlice(key)
	valueSlice = NewDataSlice(value)

	return NewTableRow(keySlice, valueSlice, uint64(timestamp), uint64(snapshotId), int8(rowType)), nil
}

func NewTableRowFromKey(key any) *TableRow {
	return NewTableRow(NewDataSlice(key), NewDataSlice([]byte{}), 0, 0, PUT)
}

// getter for key
func (i *TableRow) GetKey() *DataSlice {
	return i.key
}

// getter for value
func (i *TableRow) GetValue() *DataSlice {
	return i.value
}

// getter for timestamp
func (i *TableRow) GetTimestamp() uint64 {
	return i.timestamp
}

// getter for snapshotId
func (i *TableRow) GetSnapshotId() uint64 {
	return i.snapshotId
}

// stores the row to [writer]
func (i *TableRow) PackRow(writer io.Writer) {
	encoder := msgpack.NewEncoder(writer)
	encoder.EncodeBytes(i.GetKey().GetByte())
	encoder.EncodeInt(int64(i.GetTimestamp()))
	encoder.EncodeInt(int64(i.GetSnapshotId()))
	encoder.EncodeInt(int64(i.rowType))
	encoder.EncodeBytes(i.GetValue().GetByte())
}

func (i *TableRow) GetRowType() int {
	return int(i.rowType)
}

func (i *TableRow) String() string {
	rowType := ""
	switch i.rowType {
	case 0:
		rowType = "PUT"
	case 1:
		rowType = "DELETE"
	}

	return fmt.Sprintf("TableRow{key: %s, value: %s, timestamp: %d, snapshotId: %d, rowType: %s}", i.key.String(), i.value.String(), i.timestamp, i.snapshotId, rowType)
}

// sorts a list od TableRow
func SortTableRow(rows *[]*TableRow) {
	sorter := NewTableRowSortBy(*rows, func(a *DataSlice, b *DataSlice) int {
		return strings.Compare(string(a.String()), string(b.String()))
	})
	sort.Sort(&sorter)
}

// struct that helps in sorting TableRows
// implements Sort Interface
type TableRowSortBy struct {
	list       []*TableRow
	comparator Comparator[*DataSlice]
}

func NewTableRowSortBy(list []*TableRow, comparator Comparator[*DataSlice]) TableRowSortBy {
	return TableRowSortBy{
		list:       list,
		comparator: comparator,
	}
}

func (a *TableRowSortBy) Len() int {
	return len(a.list)
}

func (a *TableRowSortBy) Swap(i, j int) {
	a.list[i], a.list[j] = a.list[j], a.list[i]
}

func (a *TableRowSortBy) Less(i, j int) bool {
	cmp := a.comparator(a.list[i].key, a.list[j].key)

	if cmp == 0 {
		return a.list[i].snapshotId < a.list[j].snapshotId
	}

	return cmp < 0
}
