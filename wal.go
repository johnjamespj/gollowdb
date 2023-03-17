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
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Write Ahead Log is used to store the mutations to memtable
type WALWriter struct {
	id   int64
	path string
	file *os.File
}

// Creates a new log at [path] with [id]
func NewWALWriter(path string, id int64) WALWriter {
	f, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("l_%d.wal", id)), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	return WALWriter{
		id:   id,
		path: path,
		file: f,
	}
}

// Add an entry to the log
func (v WALWriter) AddTableRow(row *TableRow) {
	row.PackRow(v.file)
}

// closes the log
func (v WALWriter) Close() {
	v.file.Close()
}

// Reads a Write Ahead Log from disk
type WALReader struct {
	id   int
	path string
	file *os.File
}

// Creates a WALReader struct to read
func NewWALReader(path string, id int) WALReader {
	f, err := os.OpenFile(fmt.Sprintf("%s/l_%d.wal", path, id), os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	return WALReader{
		id:   id,
		path: path,
		file: f,
	}
}

// read all entries in the log as an array of TableRow
func (v WALReader) ReadAllRows() []*TableRow {
	v.file.Seek(0, io.SeekStart)

	reader := bufio.NewReader(v.file)
	l := make([]*TableRow, 0)

	for {
		row, err := NewTableRowFrom(reader)
		if err != nil {
			break
		}
		l = append(l, row)
	}

	return l
}

func DeleteWAL(path string, id int) {
	os.Remove(filepath.Join(path, fmt.Sprintf("l_%d.wal", id)))
}
