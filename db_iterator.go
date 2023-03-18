package gollowdb

type DBIterator struct {
	last     *DataSlice
	stop     *DataSlice
	lastRow  *TableRow
	snapshot int
	db       *DB
}

func NewDBIterator(db *DB, startKey *DataSlice, endKey *DataSlice) *DBIterator {
	return &DBIterator{
		last:     startKey,
		lastRow:  nil,
		stop:     endKey,
		snapshot: db.LockSnapshot(),
		db:       db,
	}
}

func (i *DBIterator) MoveNext() bool {
	i.db.mu.RLock()
	defer i.db.mu.RUnlock()

	if i.stop != nil && i.db.option.comparator(i.last, i.stop) >= 0 {
		return false
	}

	for {
		list := NewSortedList(make([]*TableRow, 0), func(a, b *TableRow) int {
			c := i.db.option.comparator(a.key, b.key)
			if c == 0 {
				return int(b.snapshotId) - int(a.snapshotId)
			}

			return c
		})

		// add from memtable
		itr := i.db.currentMemtable.table.Tail(&TableRow{key: i.last}, false).GetIterator()
		for itr.MoveNext() {
			var cur **TableRow
			if i.lastRow == nil {
				cur = i.db.currentMemtable.table.Ceiling(&TableRow{key: i.last})
			} else {
				cur = i.db.currentMemtable.table.Higher(&TableRow{key: i.last})
			}

			if cur != nil {
				list.AddAll(i.db.currentMemtable.table.Get(*cur).ToList())
			}
		}

		// add from immutable memtable
		for _, table := range i.db.immutablesMemtable {
			var cur **TableRow
			if i.lastRow == nil {
				cur = (*table).Ceiling(&TableRow{key: i.last})
			} else {
				cur = (*table).Higher(&TableRow{key: i.last})
			}

			if cur != nil {
				list.AddAll((*table).Get(*cur).ToList())
			}
		}

		// add from sstables
		sstables := make([]*SSTableReader, 0)
		for j := 0; j < i.db.sstableManager.LayerCount(); j++ {
			sstables = append(sstables, i.db.sstableManager.PlanSSTableQueryStrategy(i.last, j, 1)...)
		}

		for _, sstable := range sstables {
			var cur *TableRow
			if i.lastRow == nil {
				cur = sstable.Ceiling(TableRow{key: i.last})
			} else {
				cur = sstable.Higher(TableRow{key: i.last})
			}

			if cur != nil {
				list.AddAll(sstable.Get(cur).ToList())
			}
		}

		if list.GetSize() == 0 {
			i.last = nil
			i.lastRow = nil
			break
		}

		for _, row := range list.list {
			if row.snapshotId <= uint64(i.snapshot) && i.db.option.comparator(row.key, list.list[0].key) == 0 {
				i.last = row.key
				i.lastRow = row
				return true
			}
		}

		i.last = list.list[0].key
		i.lastRow = list.list[0]

		// check if we reach the end
		if i.stop != nil && i.db.option.comparator(i.last, i.stop) >= 0 {
			i.last = nil
			i.lastRow = nil
			break
		}
	}

	return false
}

func (i *DBIterator) GetCurrent() *TableRow {
	return i.lastRow
}

func (i *DBIterator) Close() {
	i.db.ReleaseSnapshot(i.snapshot)
}
