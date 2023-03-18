package gollowdb

import (
	"bytes"
	"compress/zlib"

	"github.com/golang/snappy"
)

type Compression interface {
	Encode(data []byte) []byte
	Decode(data []byte) ([]byte, error)
}

type NoCompression struct{}

func NewNoCompression() *NoCompression {
	return &NoCompression{}
}

func (c *NoCompression) Encode(data []byte) []byte {
	return data
}

func (c *NoCompression) Decode(data []byte) ([]byte, error) {
	return data, nil
}

type SnappyCompression struct{}

func NewSnappyCompression() *SnappyCompression {
	return &SnappyCompression{}
}

func (c *SnappyCompression) Encode(data []byte) []byte {
	return snappy.Encode([]byte{}, data)
}

func (c *SnappyCompression) Decode(data []byte) ([]byte, error) {
	res, err := snappy.Decode([]byte{}, data)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type ZlibCompression struct{}

func NewZlibCompression() *ZlibCompression {
	return &ZlibCompression{}
}

func (c *ZlibCompression) Encode(data []byte) []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(data)
	w.Close()
	return b.Bytes()
}

func (c *ZlibCompression) Decode(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.ReadFrom(r)
	return out.Bytes(), nil
}
