package structures

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DBOption struct {
	noCopy                 noCopy
	path                   string
	id                     string
	createIfNotExists      bool
	maxInmemoryWriteBuffer int
	comparator             Comparator[*DataSlice]
}

func NewDBOption() *DBOption {
	return &DBOption{
		path:                   ".",
		maxInmemoryWriteBuffer: 10 * 1000 * 1000,
		createIfNotExists:      false,
		comparator: func(a, b *DataSlice) int {
			return strings.Compare(string(a.String()), string(b.String()))
		},
	}
}

func (i *DBOption) SetPath(newPath string) {
	i.path = newPath
}

func (i *DBOption) SetMaxInmemoryWriteBuffer(newSize int) {
	i.maxInmemoryWriteBuffer = newSize
}

func (i *DBOption) SetShouldCreateIfNotExists() {
	i.createIfNotExists = true
}

func (i *DBOption) SetShouldNotCreateIfNotExists() {
	i.createIfNotExists = false
}

func (i *DBOption) SetComparator(comparator Comparator[*DataSlice]) {
	i.comparator = comparator
}

type DB struct {
	option               *DBOption
	manifest             *Manifest
	sstableManager       *SSTableManager
	currentWALID         int64
	currentMemtable      *SkipListMemtable
	immutablesMemtable   []*NavigableList[*TableRow]
	immutableWAL         []int
	memtableUpdateStream *chan bool
	sstableUpdateStream  *chan []SSTableUpdate
	mu                   sync.RWMutex
	waitGroup            sync.WaitGroup
}

func NewDB(option *DBOption) *DB {
	var lastSnapshotId uint64

	// check if the folder exists
	if _, err := os.Stat(filepath.Join(option.path, "MANIFEST")); errors.Is(err, os.ErrNotExist) {
		if option.createIfNotExists {
			err := os.MkdirAll(option.path, os.ModePerm)
			if err != nil {
				panic(err)
			}
		} else {
			panic("database does not exists!\ntip: Set DBOption.SetShouldCreateIfNotExists() to create if not exists!")
		}
	}

	sstableUpdateStream := make(chan []SSTableUpdate)
	manifest := OpenManifest(option.path, option.id)

	currentWALID := manifest.GetNextWALID()
	memtable := NewSkipListMemtable(option.path, currentWALID, option.comparator)
	manifest.AddWalId(int(currentWALID))
	manifest.Commit()

	lastSnapshotId = uint64(manifest.GetLastSnapshot())

	cmp := func(a *TableRow, b *TableRow) int {
		return option.comparator(a.key, b.key)
	}

	memtableUpdateStream := make(chan bool)
	db := &DB{
		sstableUpdateStream:  &sstableUpdateStream,
		option:               option,
		manifest:             manifest,
		currentWALID:         currentWALID,
		currentMemtable:      memtable,
		memtableUpdateStream: &memtableUpdateStream,
	}

	db.sstableManager = NewSSTableManager(option.path, option.comparator, manifest, cmp, &db.waitGroup)
	db.sstableManager.AddAllSSTables(manifest.GetAllTables())
	db.manifest.sstableManager = db.sstableManager

	// load all WALs
	immutableTable := NewSortedList(make([]*TableRow, 0), cmp)
	walIds := manifest.GetAllWALIds()
	for _, id := range walIds {
		if id != int(db.currentWALID) {
			walReader := NewWALReader(option.path, id)
			rows := walReader.ReadAllRows()

			if len(rows) > 0 {
				SortTableRow(&rows)
				immutableTable.Merge(rows)
				db.immutableWAL = append(db.immutableWAL, id)
			} else if len(rows) == 0 {
				manifest.RemoveWALId(id)
				manifest.Commit()
				DeleteWAL(option.path, id)
			}

			// find the max snapshotId
			for _, row := range rows {
				if row.snapshotId > uint64(lastSnapshotId) {
					lastSnapshotId = row.snapshotId
				}
			}
		}
	}

	db.currentMemtable.snapshotIdCounter = int64(lastSnapshotId)
	if immutableTable.GetSize() > 0 {
		var ptr NavigableList[*TableRow] = immutableTable
		db.immutablesMemtable = []*NavigableList[*TableRow]{&ptr}
	}

	// start background memtable flush
	go db.memtableFlushLoop(&memtableUpdateStream, &db.waitGroup)

	return db
}

func (i *DB) GetManager() *SSTableManager {
	return i.sstableManager
}

func (i *DB) Put(key any, value any) {
	i.mu.Lock()
	if i.currentMemtable.GetSize() >= i.option.maxInmemoryWriteBuffer {
		var navList NavigableList[*TableRow] = i.currentMemtable.table
		i.immutablesMemtable = append(i.immutablesMemtable, &navList)
		i.immutableWAL = append(i.immutableWAL, int(i.currentWALID))

		lastSnapshotId := i.currentMemtable.snapshotIdCounter

		currentWALID := i.manifest.GetNextWALID()
		i.currentMemtable = NewSkipListMemtable(i.option.path, currentWALID, i.option.comparator)
		i.currentMemtable.snapshotIdCounter = lastSnapshotId
		i.currentWALID = currentWALID
		i.manifest.SetLastSnapshot(lastSnapshotId)
		i.manifest.AddWalId(int(currentWALID))
		i.manifest.SetLastSnapshot(i.currentMemtable.snapshotIdCounter)
		i.manifest.Commit()

		// let background flush know
		*i.memtableUpdateStream <- true
	}
	i.mu.Unlock()

	i.currentMemtable.Put(key, value)
}

func (i *DB) Delete(key any) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	i.currentMemtable.Delete(key)
}

func (i *DB) GetImmutableMemtables() []*NavigableList[*TableRow] {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.immutablesMemtable
}

func (i *DB) Get(key any) *DataSlice {
	i.mu.RLock()
	defer i.mu.RUnlock()
	i.sstableManager.mu.RLock()
	defer i.sstableManager.mu.RUnlock()

	// Look in memtable
	list := i.currentMemtable.Get(key).ToList()
	for i := len(list) - 1; i >= 0; i-- {
		item := list[i]
		val := item.GetValue()

		if item.GetRowType() == int(DELETE) {
			return nil
		}

		if item.GetRowType() == int(PUT) {
			return val
		}
	}

	// Look in immutable memtables
	for _, tables := range i.immutablesMemtable {
		list = (*tables).Get(&TableRow{key: NewDataSlice(key)}).ToList()
		for i := len(list) - 1; i >= 0; i-- {
			item := list[i]
			val := item.GetValue()

			if item.GetRowType() == int(DELETE) {
				return nil
			}

			if item.GetRowType() == int(PUT) {
				return val
			}
		}
	}

	// read locks sstable manager
	i.sstableManager.mu.RLock()
	defer i.sstableManager.mu.RUnlock()

	// Look in sstable files
	keySlice := NewDataSlice(key)
	for j := 0; j < i.sstableManager.LayerCount(); j++ {
		tables := i.sstableManager.PlanSSTableQueryStrategy(keySlice, j)

		rows := make([]*TableRow, 0)
		for _, table := range tables {
			rows = append(rows, table.Get(&TableRow{
				key: keySlice,
			}).ToList()...)
		}

		if len(rows) != 0 {
			row := rows[len(rows)-1]
			if row.rowType == DELETE {
				return nil
			}
			return row.value
		}
	}

	return nil
}

func (i *DB) Compact() {
	*i.sstableManager.lsmUpdateStream <- true
}

func (i *DB) Close() {
	*i.memtableUpdateStream <- true
	*i.memtableUpdateStream <- false
	*i.sstableManager.lsmUpdateStream <- true
	*i.sstableManager.lsmUpdateStream <- false
	i.waitGroup.Wait()
}

func (i *DB) memtableFlushLoop(memtableUpdateStream *chan bool, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	for {
		val := <-*memtableUpdateStream
		if !val {
			waitGroup.Done()
			return
		}

		i.mu.RLock()
		i.manifest.mu.RLock()
		imm := make([]*NavigableList[*TableRow], len(i.immutablesMemtable))
		walsToRemove := make([]int, len(i.immutableWAL))
		copy(imm, i.immutablesMemtable)
		copy(walsToRemove, i.immutableWAL)
		i.manifest.mu.RUnlock()
		i.mu.RUnlock()

		// Skip flushing if no
		if len(imm) == 0 {
			continue
		}

		// save memtables to file
		sortedRows := NewSortedList(make([]*TableRow, 0), i.currentMemtable.table.comparator)
		for _, table := range imm {
			sortedRows.Merge((*table).ToList())
		}

		if len(sortedRows.list) == 0 {
			i.manifest.RemoveWALIds(walsToRemove)
			i.manifest.Commit()
			continue
		}

		newSSTableId := i.manifest.GetNextSSTableID()
		WriteSSTable(sortedRows.list, 1, 10*1000, i.option.path, 0, uint64(newSSTableId))

		// commit changes to manifest
		i.manifest.AddTable(&SSTableReferance{
			level: 0,
			id:    int(newSSTableId),
		})
		i.manifest.RemoveWALIds(walsToRemove)
		i.manifest.Commit()

		// remove memtable from array
		i.mu.Lock()

		// remove common address in i.immutablesMemtable and imm
		for _, table := range imm {
			for j, t := range i.immutablesMemtable {
				if t == table {
					i.immutablesMemtable = append(i.immutablesMemtable[:j], i.immutablesMemtable[j+1:]...)
					break
				}
			}
		}

		// remove common address in i.immutableWAL and walsToRemove
		for _, id := range walsToRemove {
			for j, t := range i.immutableWAL {
				if t == id {
					i.immutableWAL = append(i.immutableWAL[:j], i.immutableWAL[j+1:]...)
					break
				}
			}
		}

		i.mu.Unlock()
	}
}
