package structures

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexRow struct {
	key    *DataSlice
	offset int
	idx    int
}

type SSTableMetadata struct {
	filter        *Bloomfilter
	index         []*IndexRow
	minKey        *DataSlice
	maxKey        *DataSlice
	minTimestamp  uint64
	maxTimestamp  uint64
	minSnapshotID uint64
	maxSnapshotID uint64
	blockSize     uint64
	blockCount    uint64
	size          uint64
	count         uint64
}

func (i SSTableMetadata) PackSSTableMetadata(writer io.Writer) {
	encoder := msgpack.NewEncoder(writer)

	encoder.EncodeBytes(i.minKey.bin)
	encoder.EncodeBytes(i.maxKey.bin)
	encoder.EncodeInt(int64(i.minSnapshotID))
	encoder.EncodeInt(int64(i.maxSnapshotID))
	encoder.EncodeInt(int64(i.minTimestamp))
	encoder.EncodeInt(int64(i.maxTimestamp))
	encoder.EncodeInt(int64(i.blockSize))
	encoder.EncodeInt(int64(i.blockCount))
	encoder.EncodeInt(int64(i.size))
	encoder.EncodeInt(int64(i.count))

	encoder.EncodeArrayLen(len(i.index) * 2)
	for j := 0; j < len(i.index); j++ {
		encoder.EncodeBytes(i.index[j].key.bin)
		encoder.EncodeInt(int64(i.index[j].offset))
	}

	// encode filter (byteArray, hashCount)
	encoder.EncodeInt(int64(i.filter.hashCount))
	encoder.EncodeBytes(i.filter.byteArray)
}

func WriteSSTable(sorted []*TableRow, minBlockCount uint64, maxBlockSize uint64, path string, level uint64, id uint64) error {
	// extract the datapoints for the index
	metadata := extractMetadata(sorted, minBlockCount, maxBlockSize)
	blocks := extractBlocks(sorted, metadata.blockCount, metadata.blockSize)

	filename := filepath.Join(path, fmt.Sprintf("l%d_%d.sst", level, id))
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(fd)
	blockBuf, idx := packBlocks(blocks)
	metadata.index = idx
	metadata.size = uint64(len(blockBuf))
	metadata.filter = buildFilter(sorted, metadata.count)
	metadata.PackSSTableMetadata(buf)
	buf.Write(blockBuf)
	buf.Flush()

	return nil
}

func extractMetadata(sorted []*TableRow, minBlockCount uint64, maxBlockSize uint64) SSTableMetadata {
	var (
		minKey        *DataSlice
		maxKey        *DataSlice
		minTimestamp  uint64
		maxTimestamp  uint64
		minSnapshotID uint64
		maxSnapshotID uint64
		blockCount    uint64
		blockSize     uint64
		size          uint64
		count         uint64
	)

	minKey = sorted[0].key
	maxKey = sorted[len(sorted)-1].key
	count = 0

	// extract information about the list
	for _, cur := range sorted {
		count++

		if cur.timestamp < minTimestamp {
			minTimestamp = cur.timestamp
		} else if cur.timestamp > maxTimestamp {
			maxTimestamp = cur.timestamp
		}

		if cur.snapshotId < minSnapshotID {
			minSnapshotID = cur.snapshotId
		} else if cur.snapshotId > maxSnapshotID {
			maxSnapshotID = cur.snapshotId
		}

		size += uint64(8*4 + cur.key.GetSize() + cur.value.GetSize())
	}

	if size/maxBlockSize > minBlockCount {
		blockCount = uint64(size / maxBlockSize)
	} else {
		blockCount = uint64(minBlockCount)
	}

	blockSize = uint64(size) / blockCount

	return SSTableMetadata{
		minKey:        minKey,
		maxKey:        maxKey,
		size:          size,
		minTimestamp:  minTimestamp,
		maxTimestamp:  maxTimestamp,
		minSnapshotID: minSnapshotID,
		maxSnapshotID: maxSnapshotID,
		blockSize:     blockSize,
		blockCount:    blockCount,
		count:         count,
	}
}

func buildFilter(sorted []*TableRow, size uint64) *Bloomfilter {
	filter := NewBloomfilter(int(size), 10)

	for _, cur := range sorted {
		filter.Add(cur.key)
	}

	return filter
}

func extractBlocks(itr []*TableRow, blockCount uint64, blockSize uint64) [][]*TableRow {
	blocks := make([][]*TableRow, 0)
	var accumlateSize uint64 = 0

	temp := make([]*TableRow, 0)
	i := 0

	for _, cur := range itr {
		if accumlateSize >= blockSize {
			blocks = append(blocks, temp)
			i++
			accumlateSize = 0
			temp = make([]*TableRow, 0)
		}

		accumlateSize += uint64(8*4 + cur.key.GetSize() + cur.value.GetSize())
		temp = append(temp, cur)
	}

	if len(temp) > 0 {
		blocks = append(blocks, temp)
	}

	return blocks
}

func packBlocks(blocks [][]*TableRow) ([]byte, []*IndexRow) {
	idx := make([]*IndexRow, len(blocks))

	bufTotal := bytes.NewBuffer([]byte{})
	for j := 0; j < len(blocks); j++ {
		buf := bytes.NewBuffer([]byte{})
		for i := 0; i < len(blocks[j]); i++ {
			blocks[j][i].PackRow(buf)
		}
		idx[j] = &IndexRow{
			key:    blocks[j][0].key,
			offset: bufTotal.Len(),
		}
		bufTotal.Write(snappy.Encode([]byte{}, buf.Bytes()))
	}

	return bufTotal.Bytes(), idx
}
