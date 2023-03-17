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
