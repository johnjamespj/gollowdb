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
	merger                 Merger
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
		merger: &DefaultMerger{},
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
