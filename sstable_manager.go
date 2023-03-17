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
	"log"
	"os"
	"path/filepath"
	"sync"
)

type LSMLevel struct {
	levelNumber int
	fileCount   int
	levelSize   int
	score       float64
}

type SSTableReaderRef struct {
	reader *SSTableReader
	minKey *DataSlice
	maxKey *DataSlice
	level  int
	id     int
}

type SSTableManager struct {
	compactor       *Compactor
	path            string
	sstable         *SortedList[*SSTableReaderRef]
	comparator      Comparator[*DataSlice]
	mu              sync.RWMutex
	lsm             []*LSMLevel
	lsmUpdateStream *chan bool
}

func NewSSTableManager(options *DBOption, manifest *Manifest, rowComparator Comparator[*TableRow], wg *sync.WaitGroup, log *log.Logger) *SSTableManager {
	cmp := func(a *SSTableReaderRef, b *SSTableReaderRef) int {
		c := a.level - b.level
		if c == 0 {
			return options.comparator(a.minKey, b.minKey)
		}
		return c
	}

	lsmUpdateStream := make(chan bool, 100)
	manager := &SSTableManager{
		sstable:         NewSortedList(make([]*SSTableReaderRef, 0), cmp),
		comparator:      options.comparator,
		path:            options.path,
		lsm:             make([]*LSMLevel, 0),
		lsmUpdateStream: &lsmUpdateStream,
	}
	manager.compactor = NewCompactor(options, &lsmUpdateStream, manager, manifest, rowComparator, wg, log)

	return manager
}

func (i *SSTableManager) Updater(val []SSTableUpdate) {
	addList := make([]*SSTableReferance, 0)
	removeList := make([]*SSTableReferance, 0)

	for _, update := range val {
		if update.action == ADD {
			addList = append(addList, update.tables...)
		} else if update.action == REMOVE {
			removeList = append(removeList, update.tables...)
		}
	}

	i.AddAllSSTables(addList)
	i.RemoveAllSSTable(removeList)
	*i.lsmUpdateStream <- true
}

func (i *SSTableManager) AddAllSSTables(refs []*SSTableReferance) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, elm := range refs {
		sstable, err := NewSSTableReader(i.path, uint64(elm.level), uint64(elm.id), i.comparator)
		if err != nil {
			panic(err)
		}
		i.sstable.Add(&SSTableReaderRef{
			reader: &sstable,
			level:  elm.level,
			id:     elm.id,
			minKey: sstable.metadata.minKey,
			maxKey: sstable.metadata.maxKey,
		})

		// update lsm. If level is not create all level till that
		// level are created
		for len(i.lsm) <= elm.level {
			i.lsm = append(i.lsm, &LSMLevel{
				levelNumber: len(i.lsm),
				fileCount:   0,
				levelSize:   0,
				score:       0,
			})
		}

		i.lsm[elm.level].fileCount++
		i.lsm[elm.level].levelSize += int(sstable.metadata.size)
		i.lsm[elm.level].score = 0
	}
	*i.lsmUpdateStream <- true
}

func (i *SSTableManager) RemoveAllSSTable(refs []*SSTableReferance) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, elm := range refs {
		var val *SSTableReaderRef

		// remove by id and level
		list := i.sstable.list
		for i := 0; i < len(list); i++ {
			if list[i].id == elm.id && list[i].level == elm.level {
				val = list[i]
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
		i.sstable.list = list

		if val == nil {
			panic(fmt.Sprintf("SSTable with id %d and level %d not found", elm.id, elm.level))
		}
		(*val).reader.file.Close()

		i.lsm[elm.level].fileCount--
		i.lsm[elm.level].levelSize -= int((*val).reader.metadata.size)
		i.lsm[elm.level].score = 0

		*i.lsmUpdateStream <- true
	}
	i.DeleteSSTablesByIds(refs)
}

func (i *SSTableManager) PlanSSTableQueryStrategy(key *DataSlice, ln int) []*SSTableReader {
	i.mu.RLock()
	defer i.mu.RUnlock()

	files := i.GetFilesFromLayer(ln)
	readers := make([]*SSTableReader, 0)

	for _, curr := range files {
		if i.comparator(curr.minKey, key) <= 0 && i.comparator(curr.maxKey, key) >= 0 {
			readers = append(readers, curr.reader)
		}
	}

	return readers
}

func (i *SSTableManager) FilesInRangeInLayer(start *DataSlice, end *DataSlice, ln int) []*SSTableReader {
	i.mu.RLock()
	defer i.mu.RUnlock()

	files := i.GetFilesFromLayer(ln)
	readers := make([]*SSTableReader, 0)

	for _, curr := range files {
		if i.comparator(curr.minKey, start) <= 0 && i.comparator(curr.maxKey, end) >= 0 {
			readers = append(readers, curr.reader)
		}
	}

	return readers
}

func (i *SSTableManager) GetFilesFromLayer(ln int) []*SSTableReaderRef {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var layer []*SSTableReaderRef

	itr := i.sstable.GetIterator()
	for itr.MoveNext() {
		current := itr.GetCurrent()

		if current.level == ln {
			layer = append(layer, current)
		}

		if current.level > ln {
			break
		}
	}

	return layer
}

func (i *SSTableManager) GetFileCount() int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.sstable.GetSize()
}

func (i *SSTableManager) DeleteSSTablesByIds(ids []*SSTableReferance) {
	for _, elm := range ids {
		// delets the file
		filename := filepath.Join(i.path, fmt.Sprintf("l%d_%d.sst", elm.level, elm.id))
		err := os.Remove(filename)
		if err != nil {
			panic(err)
		}
	}
}

func (i *SSTableManager) LayerCount() int {
	return len(i.lsm)
}
