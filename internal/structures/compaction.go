package structures

import (
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

type Compactor struct {
	logger          *log.Logger
	manifest        *Manifest
	sstableManager  *SSTableManager
	taskQueue       *SortedList[*LSMLevel]
	lsmUpdateStream *chan bool
	strategy        CompactionStrategy
	comparator      Comparator[*TableRow]
	wg              *sync.WaitGroup
	mu              sync.RWMutex
	levelZero       sync.Mutex

	levelZeroStream  *chan bool
	compactionStream *chan bool
	merger           Merger
}

func NewCompactor(options *DBOption, lsmUpdateStream *chan bool, sstableManager *SSTableManager, manifest *Manifest, comparator Comparator[*TableRow], wg *sync.WaitGroup, log *log.Logger) *Compactor {
	cmp := func(a *LSMLevel, b *LSMLevel) int {
		return int((a.score - b.score) * 1000)
	}

	var strategy CompactionStrategy = &LeveledCompaction{
		options: options.lsmOptions,
	}

	compactionStream := make(chan bool)
	levelZeroStream := make(chan bool)

	compactor := &Compactor{
		logger:           log,
		taskQueue:        NewSortedList(make([]*LSMLevel, 0), cmp),
		lsmUpdateStream:  lsmUpdateStream,
		sstableManager:   sstableManager,
		strategy:         strategy,
		manifest:         manifest,
		comparator:       comparator,
		wg:               wg,
		compactionStream: &compactionStream,
		levelZeroStream:  &levelZeroStream,
		merger:           options.merger,
	}

	go func() {
		for {
			res := <-*lsmUpdateStream
			compactionStream <- res
			levelZeroStream <- res
		}
	}()

	compactor.Start()

	return compactor
}

func (i *Compactor) CalculateScore() {
	i.mu.Lock()
	i.sstableManager.mu.Lock()
	defer i.mu.Unlock()
	defer i.sstableManager.mu.Unlock()

	for _, level := range i.sstableManager.lsm {
		level.score = i.strategy.CalculateScore(i.sstableManager.lsm, level.levelNumber)
	}
}

func (i *Compactor) CreateTaskList() []*LSMLevel {
	i.sstableManager.mu.RLock()
	defer i.sstableManager.mu.RUnlock()

	l := make([]*LSMLevel, len(i.sstableManager.lsm))
	copy(l, i.sstableManager.lsm)
	// sort by score
	sort.Slice(l, func(i, j int) bool {
		return l[i].score > l[j].score
	})

	// remove tasks that have score less than or equal to 1
	for i, level := range l {
		if level.score <= 1 {
			l = l[:i]
			break
		}
	}

	return l
}

func (i *Compactor) Start() {
	go i.RunCompactor()
	go i.RunLevelZeroCompaction()
}

func (i *Compactor) RunCompactor() {
	i.wg.Add(1)
	for {
		l := <-*i.compactionStream
		if !l {
			i.wg.Done()
			return
		}

		i.CalculateScore()
		tasks := i.CreateTaskList()

		i.levelZero.Lock()

		if len(tasks) == 0 {
			i.levelZero.Unlock()
			continue
		}

		lvl := tasks[0].levelNumber
		score := tasks[0].score

		delta := int(i.strategy.CalculateDelta(i.sstableManager.lsm, lvl)/i.strategy.PartitionSize(i.sstableManager.lsm, lvl)) + 1

		if score <= 1 || lvl == 0 {
			i.levelZero.Unlock()
			continue
		}

		lvlFiles := i.sstableManager.GetFilesFromLayer(lvl)

		if delta >= len(lvlFiles) {
			i.levelZero.Unlock()
			continue
		}

		startTime := time.Now().UnixNano()

		lvlFiles = lvlFiles[:delta]

		// ids of files from lvl
		fromFilesIds := make([]int, 0)
		for _, f := range lvlFiles {
			fromFilesIds = append(fromFilesIds, int(f.id))
		}

		minKey := lvlFiles[0].minKey
		maxKey := lvlFiles[len(lvlFiles)-1].maxKey

		readers := i.sstableManager.FilesInRangeInLayer(minKey, maxKey, lvl+1)
		// ids of files from lvl+1
		toFilesIds := make([]int, 0)
		for _, reader := range readers {
			lvlFiles = append(lvlFiles, &SSTableReaderRef{
				minKey: reader.metadata.minKey,
				maxKey: reader.metadata.maxKey,
				reader: reader,
				level:  int(reader.level),
				id:     int(reader.id),
			})
			toFilesIds = append(toFilesIds, int(reader.id))
		}

		row := i.SSTableLoader(lvlFiles)
		refs := i.SplitAndSave(row, i.strategy.PartitionSize(i.sstableManager.lsm, lvl), lvl+1)

		// new files
		newFilesIds := make([]int, 0)
		for _, ref := range refs {
			newFilesIds = append(newFilesIds, ref.id)
		}

		// request file removal
		sstableRefs := make([]*SSTableReferance, 0)
		for _, item := range lvlFiles {
			sstableRefs = append(sstableRefs, &SSTableReferance{
				level: item.level,
				id:    item.id,
			})
		}
		i.manifest.RemoveTables(sstableRefs)
		i.manifest.Commit()
		i.levelZero.Unlock()

		i.logger.Printf(
			"LevelNCompaction(L%d->L%d){TimeTaken: %fs, filesImpacted: %d, from:L%d: %v, to:L%d: %v, new_files:L%d: %v}",
			lvl, lvl+1,
			float64(time.Now().UnixNano()-startTime)/float64(time.Second),
			len(lvlFiles)+len(refs),
			lvl, fromFilesIds,
			lvl+1, toFilesIds,
			lvl+1, newFilesIds,
		)
	}
}

func (i *Compactor) RunLevelZeroCompaction() {
	i.wg.Add(1)
	for {
		l := <-*i.levelZeroStream
		if !l {
			i.wg.Done()
			return
		}

		// calculate levelZero score
		score := i.strategy.CalculateScore(i.sstableManager.lsm, 0)

		if score > 1 {
			i.levelZero.Lock()
			startTime := time.Now().UnixNano()

			l0 := i.sstableManager.GetFilesFromLayer(0)
			l1 := i.sstableManager.GetFilesFromLayer(1)

			files := make([]*SSTableReaderRef, 0)
			files = append(files, l0...)
			files = append(files, l1...)

			row := i.SSTableLoader(files)
			refs := i.SplitAndSave(row, i.strategy.PartitionSize(i.sstableManager.lsm, 1), 1)

			// request file removal
			sstableRefs := make([]*SSTableReferance, 0)
			for _, item := range files {
				sstableRefs = append(sstableRefs, &SSTableReferance{
					level: item.level,
					id:    item.id,
				})
			}

			i.manifest.RemoveTables(sstableRefs)
			i.manifest.Commit()
			i.levelZero.Unlock()

			from := make([]int, 0)
			for _, l := range l0 {
				from = append(from, l.id)
			}

			to := make([]int, 0)
			for _, l := range l1 {
				to = append(to, l.id)
			}

			newFiles := make([]int, 0)
			for _, l := range refs {
				newFiles = append(newFiles, l.id)
			}

			i.logger.Printf(
				"LevelZeroCompaction(L0->L1){ TimeTaken: %fs, filesImpacted: %d, from:L0: %v, to:L1: %v, new_files:L1: %v }",
				float64(time.Now().UnixNano()-startTime)/float64(time.Second),
				len(refs)+len(files),
				from, to, newFiles,
			)
		}
	}
}

func (i *Compactor) SSTableLoader(tables []*SSTableReaderRef) []*TableRow {
	var (
		wg  sync.WaitGroup
		res chan []*TableRow
	)

	res = make(chan []*TableRow, len(tables))
	for _, table := range tables {
		wg.Add(1)

		// parallel reads
		go func(t *SSTableReaderRef) {
			i.sstableManager.mu.RLock()
			defer i.sstableManager.mu.RUnlock()
			defer wg.Done()

			res <- t.reader.ToList()
		}(table)
	}

	wg.Wait()

	// sort and merge all the rowss
	list := make([]*TableRow, 0)
	for j := 0; j < len(tables); j++ {
		r := <-res
		list = Merge(list, r, i.comparator)
	}

	// merge rows with same key
	newList := make([]*TableRow, 0)
	cur := list[0]
	acc := make([]*TableRow, 1)
	acc[0] = cur
	for j := 1; j < len(list); j++ {
		row := list[j]
		if i.sstableManager.comparator(cur.key, row.key) == 0 {
			acc = append(acc, row)
		} else {
			if len(acc) > 1 {
				acc = i.merger.MergeRows(acc)
			}

			newList = append(newList, acc...)

			acc = make([]*TableRow, 1)
			acc[0] = row
			cur = row
		}
	}

	return newList
}

func Merge(a []*TableRow, b []*TableRow, comparator Comparator[*TableRow]) []*TableRow {
	newList := make([]*TableRow, 0)
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if comparator(a[i], b[j]) < 0 {
			newList = append(newList, a[i])
			i++
		} else {
			newList = append(newList, b[j])
			j++
		}
	}

	for i < len(a) {
		newList = append(newList, a[i])
		i++
	}

	for j < len(b) {
		newList = append(newList, b[j])
		j++
	}

	return newList
}

func (i *Compactor) SplitAndSave(sorted []*TableRow, partitionSize int, writeLevel int) []*SSTableReferance {
	totalSize := 0
	for _, item := range sorted {
		totalSize += item.key.GetSize() + 8*4

		if item.value != nil {
			totalSize += item.value.GetSize()
		}
	}

	acc := 0

	// write to partitions
	var wg sync.WaitGroup
	ids := make([]*SSTableReferance, 0)
	lastIdx := 0
	for j, item := range sorted {
		if acc >= partitionSize {
			wg.Add(1)
			id := i.manifest.GetNextSSTableID()
			go func(a []*TableRow, id int64) {
				WriteSSTable(a, 1, 10*1000, i.sstableManager.path, uint64(writeLevel), uint64(id))
				wg.Done()
			}(sorted[lastIdx:j], id)
			ids = append(ids, &SSTableReferance{
				level: writeLevel,
				id:    int(id),
			})

			acc = 0
			lastIdx = j
		}

		acc += item.key.GetSize() + item.value.GetSize() + 8*4
	}

	// check if there is a remainder
	if lastIdx < len(sorted) {
		wg.Add(1)
		id := i.manifest.GetNextSSTableID()
		go func(a []*TableRow, id int64) {
			WriteSSTable(a, 1, 10*1000, i.sstableManager.path, uint64(writeLevel), uint64(id))
			wg.Done()
		}(sorted[lastIdx:], id)
		ids = append(ids, &SSTableReferance{
			level: writeLevel,
			id:    int(id),
		})
	}

	// wait for all writers
	wg.Wait()
	i.manifest.AddTables(ids)
	return ids
}

type CompactionStrategy interface {
	CalculateScore(lsm []*LSMLevel, level int) float64
	CalculateDelta(lsm []*LSMLevel, level int) int
	PartitionSize(lsm []*LSMLevel, level int) int
}

type LeveledCompaction struct {
	options LSMOptions
}

func (i *LeveledCompaction) CalculateScore(lsm []*LSMLevel, level int) float64 {
	if len(lsm) <= 0 {
		return 0
	}

	if level == 0 {
		return float64(lsm[0].fileCount) / float64(i.options.maxLevelZeroFileCount)
	} else {
		return float64(lsm[level].levelSize) / float64(i.CalculateLevelTargetSize(level))
	}
}

func (i *LeveledCompaction) CalculateDelta(lsm []*LSMLevel, level int) int {
	if len(lsm) <= 0 {
		return 0
	}

	if level == 0 && lsm[0].fileCount <= i.options.maxLevelZeroFileCount {
		return 0
	}

	return lsm[level].levelSize - i.CalculateLevelTargetSize(level)
}

func (i *LeveledCompaction) PartitionSize(lsm []*LSMLevel, level int) int {
	return i.options.sstableFileSize
}

func (i *LeveledCompaction) CalculateLevelTargetSize(level int) int {
	if level < 1 {
		return 0
	}
	return i.options.maxBaseLevelSize * int(math.Pow(float64(i.options.levelFactor), float64(level-1)))
}

type Merger interface {
	MergeRows(rows []*TableRow) []*TableRow
}

type DefaultMerger struct{}

func (i *DefaultMerger) MergeRows(rows []*TableRow) []*TableRow {
	// remove all if the last row is DELETE
	if rows[len(rows)-1].rowType == DELETE {
		return []*TableRow{}
	}

	return []*TableRow{rows[len(rows)-1]}
}
