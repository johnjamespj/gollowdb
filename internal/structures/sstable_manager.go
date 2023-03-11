package structures

import (
	"fmt"
	"sort"
	"sync"
)

type SSTableReaderRef struct {
	reader SSTableReader
	minKey *DataSlice
	maxKey *DataSlice
	level  int
	id     int
}

type SSTableManager struct {
	sstableUpdateStream *chan []SSTableUpdate
	path                string
	sstable             SortedList[SSTableReaderRef]
	comparator          Comparator[DataSlice]
	mu                  sync.RWMutex
}

func NewSSTableManager(path string, comparator Comparator[DataSlice], sstableUpdateStream *chan []SSTableUpdate) *SSTableManager {
	cmp := func(a SSTableReaderRef, b SSTableReaderRef) int {
		if a.id == b.id {
			return a.level - b.level
		}
		return comparator(*a.minKey, *b.minKey)
	}

	manager := &SSTableManager{
		sstableUpdateStream: sstableUpdateStream,
		sstable:             NewSortedList(make([]SSTableReaderRef, 0), cmp),
		comparator:          comparator,
		path:                path,
	}

	go manager.Updater()

	return manager
}

func (i *SSTableManager) Updater() {
	for {
		val := <-*i.sstableUpdateStream

		addList := make([]SSTableReferance, 0)
		removeList := make([]SSTableReferance, 0)

		for _, update := range val {
			if update.action == ADD {
				addList = append(addList, update.tables...)
			} else if update.action == REMOVE {
				removeList = append(removeList, update.tables...)
			}
		}

		i.AddAllSSTables(addList)
		i.RemoveAllSSTable(removeList)
		fmt.Printf("(sstable_manage) add: %d; remove: %d sstableFiles: %d\n", len(addList), len(removeList), i.sstable.GetSize())
	}
}

func (i *SSTableManager) AddAllSSTables(refs []SSTableReferance) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, elm := range refs {
		sstable, err := NewSSTableReader(i.path, uint64(elm.level), uint64(elm.id), i.comparator)
		if err != nil {
			panic(err)
		}
		i.sstable.Add(SSTableReaderRef{
			reader: sstable,
			level:  elm.level,
			id:     elm.id,
			minKey: &sstable.metadata.minKey,
			maxKey: &sstable.metadata.maxKey,
		})
	}
}

func (i *SSTableManager) RemoveAllSSTable(refs []SSTableReferance) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, elm := range refs {
		i.sstable.Remove(SSTableReaderRef{
			id:    elm.id,
			level: elm.level,
		})
	}
}

func (i *SSTableManager) PlanSSTableQueryStrategy(key DataSlice) []*SSTableReader {
	i.mu.RLock()
	defer i.mu.RUnlock()

	itr := i.sstable.GetIterator()

	sstables := make([]*SSTableReader, 0)

	for itr.MoveNext() {
		current := itr.GetCurrent()
		if i.comparator(*current.minKey, key) > 0 {
			break
		}
		sstables = append(sstables, &current.reader)
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].level < sstables[j].level
	})

	return sstables
}
