package structures

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type DBOption struct {
	path                   string
	id                     string
	createIfNotExists      bool
	maxInmemoryWriteBuffer int
	comparator             Comparator[DataSlice]
}

func NewDBOption() DBOption {
	return DBOption{
		path:                   ".",
		maxInmemoryWriteBuffer: 10 * 1000 * 1000,
		createIfNotExists:      false,
		comparator: func(a, b DataSlice) int {
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

func (i *DBOption) SetComparator(comparator Comparator[DataSlice]) {
	i.comparator = comparator
}

type DB struct {
	option              DBOption
	manifest            *Manifest
	sstableManager      *SSTableManager
	currentWALID        int64
	currentMemtable     *SkipListMemtable
	immutablesMemtable  []*NavigableList[TableRow]
	sstableUpdateStream chan []SSTableUpdate
	mu                  sync.RWMutex
}

func NewDB(option DBOption) *DB {
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
	sstableManager := NewSSTableManager(option.path, option.comparator, &sstableUpdateStream)
	manifest := OpenManifest(option.path, option.id, &sstableUpdateStream)

	currentWALID := manifest.GetNextWALID()
	memtable := NewSkipListMemtable(option.path, currentWALID, option.comparator)
	manifest.AddWalId(int(currentWALID))
	manifest.Commit()

	lastSnapshotId = uint64(manifest.GetLastSnapshot())

	if manifest.sstables.GetSize() > 0 {
		sstableUpdateStream <- []SSTableUpdate{
			{
				action: ADD,
				tables: manifest.GetAllTables(),
			},
		}
	}

	cmp := func(a TableRow, b TableRow) int {
		return option.comparator(a.key, b.key)
	}

	db := &DB{
		sstableUpdateStream: sstableUpdateStream,
		sstableManager:      sstableManager,
		option:              option,
		manifest:            manifest,
		currentWALID:        currentWALID,
		currentMemtable:     memtable,
	}

	// load all WALs
	immutableTables := make([]*NavigableList[TableRow], 0)
	walIds := manifest.GetAllWALIds()
	for _, id := range walIds {
		if id != int(db.currentWALID) {
			walReader := NewWALReader(option.path, id)
			rows := walReader.ReadAllRows()
			SortTableRow(&rows)
			list := NewSortedList(rows, cmp)

			if len(rows) > 0 {
				var table NavigableList[TableRow] = &list
				immutableTables = append(immutableTables, &table)
				go db.flushMemtable(&table, id)
			} else {
				manifest.RemoveId(id)
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

	manifest.Print()

	db.currentMemtable.snapshotIdCounter = int64(lastSnapshotId)
	db.immutablesMemtable = immutableTables
	return db
}

func (i *DB) Put(key any, value any) {
	i.mu.Lock()
	if i.currentMemtable.GetSize() >= i.option.maxInmemoryWriteBuffer {
		var table NavigableList[TableRow] = i.currentMemtable.table
		go i.flushMemtable(&table, int(i.currentWALID))

		var navList NavigableList[TableRow] = i.currentMemtable.table
		i.immutablesMemtable = append(i.immutablesMemtable, &navList)

		lastSnapshotId := i.currentMemtable.snapshotIdCounter

		currentWALID := i.manifest.GetNextWALID()
		i.currentMemtable = NewSkipListMemtable(i.option.path, currentWALID, i.option.comparator)
		i.currentMemtable.snapshotIdCounter = lastSnapshotId
		i.manifest.SetLastSnapshot(lastSnapshotId)
		i.manifest.AddWalId(int(currentWALID))
		i.manifest.SetLastSnapshot(i.currentMemtable.snapshotIdCounter)
		i.manifest.Commit()
	}
	i.mu.Unlock()

	i.currentMemtable.Put(key, value)
}

func (i *DB) Delete(key any) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	i.currentMemtable.Delete(key)
}

func (i *DB) Get(key any) *DataSlice {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Look in memtable
	list := i.currentMemtable.Get(key).ToList()
	for i := len(list) - 1; i >= 0; i-- {
		item := list[i]
		val := item.GetValue()

		if item.GetRowType() == int(DELETE) {
			return nil
		}

		if item.GetRowType() == int(PUT) {
			return &val
		}
	}

	// Look in immutable table
	for _, tables := range i.immutablesMemtable {
		list = (*tables).Get(TableRow{key: NewDataSlice(key)}).ToList()
		for i := len(list) - 1; i >= 0; i-- {
			item := list[i]
			val := item.GetValue()

			if item.GetRowType() == int(DELETE) {
				return nil
			}

			if item.GetRowType() == int(PUT) {
				return &val
			}
		}
	}

	// Look in sstable files
	readers := i.sstableManager.PlanSSTableQueryStrategy(NewDataSlice(key))

	// look at level 0
	list = make([]TableRow, 0)
	j := 0
	for ; j < len(readers); j++ {
		reader := readers[j]
		if reader.level != 0 {
			break
		}
		list = append(list, reader.Get(TableRow{key: NewDataSlice(key)}).ToList()...)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].snapshotId < list[j].snapshotId
	})

	for i := len(list) - 1; i >= 0; i-- {
		item := list[i]
		val := item.GetValue()

		if item.GetRowType() == int(DELETE) {
			return nil
		}

		if item.GetRowType() == int(PUT) {
			return &val
		}
	}

	// look at other level
	list = make([]TableRow, 0)
	for ; j < len(readers); j++ {
		reader := readers[j]
		list = reader.Get(TableRow{key: NewDataSlice(key)}).ToList()

		for i := len(list) - 1; i >= 0; i-- {
			item := list[i]
			val := item.GetValue()

			if item.GetRowType() == int(DELETE) {
				return nil
			}

			if item.GetRowType() == int(PUT) {
				return &val
			}
		}
	}

	return nil
}

func (i *DB) flushMemtable(table *NavigableList[TableRow], walId int) {
	id := uint64(i.manifest.GetNextSSTableID())
	err := WriteSSTable(*table, 10, 1000*1000, i.option.path, 0, id)
	if err != nil {
		panic(err)
	}
	i.manifest.AddTable(SSTableReferance{
		level: 0,
		id:    int(id),
	})

	i.manifest.RemoveId(walId)
	i.manifest.Commit()
	DeleteWAL(i.option.path, walId)
}
