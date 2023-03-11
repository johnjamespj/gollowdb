package structures

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type Manifest struct {
	path                string
	sstableIdCounter    int64
	walIdCounter        int64
	version             int64
	id                  string
	sstables            SortedList[SSTableReferance]
	walIds              SortedList[int]
	sstableUpdateStream *chan []SSTableUpdate
	lastSnapshot        int64
	mu                  sync.RWMutex

	bufferedUpdate []SSTableUpdate
}

func OpenManifest(path string, id string, sstableUpdateStream *chan []SSTableUpdate) *Manifest {
	var manifest Manifest
	if _, err := os.Stat(filepath.Join(path, "MANIFEST")); errors.Is(err, os.ErrNotExist) {
		// create a new manifest object
		sstableList := make([]SSTableReferance, 0)
		walIds := make([]int, 0)

		manifest = Manifest{
			sstableUpdateStream: sstableUpdateStream,
			path:                path,
			sstableIdCounter:    0,
			walIdCounter:        0,
			version:             0,
			id:                  id,
			sstables: NewSortedList(sstableList, func(a SSTableReferance, b SSTableReferance) int {
				c := a.level - b.level

				if c == 0 {
					return a.id - b.id
				}

				return c
			}),
			walIds: NewSortedList(walIds, func(a int, b int) int { return a - b }),
		}

		buf := bytes.NewBuffer([]byte{})
		manifest.PackSSTableMetadata(buf)

		// create ${path}/MANIFEST and write
		err := os.WriteFile(filepath.Join(path, "MANIFEST"), buf.Bytes(), 0644)
		if err != nil {
			panic(fmt.Sprintf("error renaming file: %s \nReasons could be: OpenManifest() is not used to create manifest", err))
		}
	} else {
		file, err := os.Open(filepath.Join(path, "MANIFEST"))
		if err != nil {
			panic("Cannot open MANIFEST file")
		}

		manifest, err = unpackManifest(file)
		if err != nil {
			// TODO: recover from the old manifest
			panic("MANIFEST reading error (TODO: recover from the old manifest)")
		}

		manifest.path = path
		manifest.sstableUpdateStream = sstableUpdateStream
	}

	return &manifest
}

func unpackManifest(reader io.Reader) (Manifest, error) {
	decoder := msgpack.NewDecoder(reader)

	var (
		id               string
		version          int
		sstableIdCounter int
		walIdCounter     int
		lastSnapshot     int
		walLen           int
		walIds           []int
		sstableLen       int
		sstables         []SSTableReferance
		err              error
	)

	id, err = decoder.DecodeString()
	if err != nil {
		return Manifest{}, err
	}

	version, err = decoder.DecodeInt()
	if err != nil {
		return Manifest{}, err
	}

	sstableIdCounter, err = decoder.DecodeInt()
	if err != nil {
		return Manifest{}, err
	}

	walIdCounter, err = decoder.DecodeInt()
	if err != nil {
		return Manifest{}, err
	}

	lastSnapshot, err = decoder.DecodeInt()
	if err != nil {
		return Manifest{}, err
	}

	walLen, err = decoder.DecodeArrayLen()
	if err != nil {
		return Manifest{}, err
	}

	walIds = make([]int, walLen)
	for i := 0; i < walLen; i++ {
		walIds[i], err = decoder.DecodeInt()
		if err != nil {
			return Manifest{}, err
		}
	}

	sstableLen, err = decoder.DecodeArrayLen()
	if err != nil {
		return Manifest{}, err
	}
	sstableLen = sstableLen / 2

	sstables = make([]SSTableReferance, sstableLen)
	for i := 0; i < sstableLen; i++ {
		id, err := decoder.DecodeInt()
		if err != nil {
			return Manifest{}, err
		}

		level, err := decoder.DecodeInt()
		if err != nil {
			return Manifest{}, err
		}

		sstables[i] = SSTableReferance{
			id:    id,
			level: level,
		}
	}

	return Manifest{
		id:               id,
		version:          int64(version),
		lastSnapshot:     int64(lastSnapshot),
		sstableIdCounter: int64(sstableIdCounter),
		walIdCounter:     int64(walIdCounter),
		walIds:           NewSortedList(walIds, func(a int, b int) int { return a - b }),
		sstables: NewSortedList(sstables, func(a SSTableReferance, b SSTableReferance) int {
			c := a.level - b.level

			if c == 0 {
				return a.id - b.id
			}

			return c
		}),
	}, nil
}

func (i *Manifest) PackSSTableMetadata(writer io.Writer) {
	encoder := msgpack.NewEncoder(writer)

	encoder.Encode(i.id)
	encoder.Encode(i.version)
	encoder.Encode(i.sstableIdCounter)
	encoder.Encode(i.walIdCounter)
	encoder.Encode(i.lastSnapshot)

	// pack wal list
	encoder.Encode(i.walIds.list)

	// pack sstable list
	encoder.EncodeArrayLen(i.sstables.GetSize() * 2)
	itr := i.sstables.GetIterator()
	for itr.MoveNext() {
		current := itr.GetCurrent()
		encoder.Encode(current.id)
		encoder.Encode(current.level)
	}
}

func (m *Manifest) Commit() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// assumes ${path}/MANIFEST exists
	err := os.Rename(filepath.Join(m.path, "MANIFEST"), filepath.Join(m.path, "MANIFEST.old"))
	if err != nil {
		panic(fmt.Sprintf("error renaming file: %s \nReasons could be: OpenManifest() is not used to create manifest", err))
	}

	// commit the update to manager
	if len(m.bufferedUpdate) > 0 {
		*m.sstableUpdateStream <- m.bufferedUpdate
		m.bufferedUpdate = make([]SSTableUpdate, 0)
	}

	buf := bytes.NewBuffer([]byte{})
	m.PackSSTableMetadata(buf)

	// write to ${path}/MANIFEST file
	err = os.WriteFile(filepath.Join(m.path, "MANIFEST"), buf.Bytes(), 0644)
	if err != nil {
		panic(err)
	}

	os.Remove(filepath.Join(m.path, "MANIFEST.old"))
	m.version++
}

func (m *Manifest) GetLastSnapshot() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return int(m.lastSnapshot)
}

func (m *Manifest) SetLastSnapshot(snapshot int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastSnapshot = snapshot
}

func (m *Manifest) AddTables(tables []SSTableReferance) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bufferedUpdate = append(m.bufferedUpdate, SSTableUpdate{
		action: ADD,
		tables: tables,
	})

	m.sstables.AddAll(tables)
}

func (m *Manifest) AddTable(table SSTableReferance) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bufferedUpdate = append(m.bufferedUpdate, SSTableUpdate{
		action: ADD,
		tables: []SSTableReferance{table},
	})

	m.sstables.Add(table)
}

func (m *Manifest) GetAllTables() []SSTableReferance {
	return m.sstables.list
}

func (m *Manifest) RemoveTable(table SSTableReferance) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bufferedUpdate = append(m.bufferedUpdate, SSTableUpdate{
		action: REMOVE,
		tables: []SSTableReferance{table},
	})

	m.sstables.Remove(table)
}

func (m *Manifest) RemoveTables(tables []SSTableReferance) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bufferedUpdate = append(m.bufferedUpdate, SSTableUpdate{
		action: REMOVE,
		tables: tables,
	})

	for _, elm := range tables {
		m.sstables.Remove(elm)
	}
}

func (m *Manifest) GetAllWALIds() []int {
	return m.walIds.list
}

func (m *Manifest) AddWalIds(ids []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.walIds.AddAll(ids)
}

func (m *Manifest) AddWalId(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.walIds.Add(id)
}

func (m *Manifest) RemoveId(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.walIds.Remove(id)
}

func (m *Manifest) RemoveIds(ids []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, elm := range ids {
		m.walIds.Remove(elm)
	}
}

func (m *Manifest) GetNextSSTableID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sstableIdCounter++
	return m.sstableIdCounter
}

func (m *Manifest) GetNextWALID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.walIdCounter++
	return m.walIdCounter
}

// getter for iterationId
func (m *Manifest) GetVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.version
}

// getter for id
func (m *Manifest) GetDBid() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.id
}

func (m *Manifest) Print() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Println("SSTable Id Counter: ", m.sstableIdCounter)
	fmt.Println("WAL Id Counter: ", m.walIdCounter)
	fmt.Println("SSTable List: ", m.sstables.list)
	fmt.Println("WAL List: ", m.walIds.list)
}

type SSTableReferance struct {
	level int
	id    int
}

func NewSSTableRef(level int, id int) *SSTableReferance {
	return &SSTableReferance{level: level, id: id}
}

func (i *SSTableReferance) GetLevel() int {
	return i.level
}

func (i *SSTableReferance) GetId() int {
	return i.id
}

type WALReference struct {
	id int
}

func (i *WALReference) GetId() int {
	return i.id
}

const (
	ADD    = 0
	REMOVE = 1
)

type SSTableUpdate struct {
	action int
	tables []SSTableReferance
}
