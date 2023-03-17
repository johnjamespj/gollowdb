package structures

import (
	"log"
	"strings"
)

type DBOption struct {
	lsmOptions             LSMOptions
	logger                 *log.Logger
	noCopy                 noCopy
	path                   string
	id                     *string
	createIfNotExists      bool
	maxInmemoryWriteBuffer int
	comparatorName         string
	comparator             Comparator[*DataSlice]
}

func NewDBOption() *DBOption {
	return &DBOption{
		path:                   "data",
		maxInmemoryWriteBuffer: 10 * 1000 * 1000,
		createIfNotExists:      false,
		comparatorName:         "default",
		comparator: func(a, b *DataSlice) int {
			return strings.Compare(string(a.String()), string(b.String()))
		},
		lsmOptions: LSMOptions{
			maxLevelZeroFileCount: 5,
			maxBaseLevelSize:      10 * 20 * 1000 * 1000,
			levelFactor:           2,
			sstableFileSize:       20 * 1000 * 1000,
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

func (i *DBOption) SetComparator(name string, comparator Comparator[*DataSlice]) {
	i.comparatorName = name
	i.comparator = comparator
}

func (i *DBOption) SetLSMOptions(options LSMOptions) {
	i.lsmOptions = options
}

type LSMOptions struct {
	maxBaseLevelSize      int
	levelFactor           int
	maxLevelZeroFileCount int
	sstableFileSize       int
}

func NewLSMOptions(maxBaseLevelSize int, levelFactor int, maxLevelZeroFileCount int, sstableFileSize int) LSMOptions {
	return LSMOptions{
		maxBaseLevelSize:      maxBaseLevelSize,
		levelFactor:           levelFactor,
		maxLevelZeroFileCount: maxLevelZeroFileCount,
		sstableFileSize:       sstableFileSize,
	}
}

func (i *LSMOptions) SetMaxBaseLevelSize(maxBaseLevelSize int) {
	i.maxBaseLevelSize = maxBaseLevelSize
}

func (i *LSMOptions) GetMaxBaseLevelSize(maxBaseLevelSize int) int {
	return maxBaseLevelSize
}

func (i *LSMOptions) SetLevelFactor(levelFactor int) {
	i.levelFactor = levelFactor
}

func (i *LSMOptions) GetLevelFactor(levelFactor int) int {
	return levelFactor
}

func (i *LSMOptions) SetMaxLevelZeroFileCount(maxLevelZeroFileCount int) {
	i.maxLevelZeroFileCount = maxLevelZeroFileCount
}

func (i *LSMOptions) GetMaxLevelZeroFileCount(maxLevelZeroFileCount int) int {
	return maxLevelZeroFileCount
}

func (i *LSMOptions) SetSSTableFileSize(sstableFileSize int) {
	i.sstableFileSize = sstableFileSize
}

func (i *LSMOptions) GetSSTableFileSize(sstableFileSize int) int {
	return sstableFileSize
}
