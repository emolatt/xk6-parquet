package main

import (
	"bytes"
	"errors"
	"io"

	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/reader"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/modules/k6"

	"github.com/dop251/goja"
)

func init() {
	modules.Register("k6/x/parquet", New())
}

// Parquet is the k6 extension module
type Parquet struct{}

// Ensure Parquet implements the modules.Instance interface
var _ modules.Instance = &Parquet{}

// New returns a new instance of the parquet module
func New() modules.Instance {
	return &Parquet{}
}

// Exports returns the exported functions from the module
func (p *Parquet) Exports() modules.Exports {
	return modules.Exports{
		Default: &ParquetModule{},
	}
}

// ParquetModule contains the actual functionality exposed to JS
type ParquetModule struct {
	vu modules.VU
}

func (p *ParquetModule) SetVU(vu modules.VU) {
	p.vu = vu
}

func (p *ParquetModule) Reset() {}

func (p *ParquetModule) ReadParquetBytes(data goja.Value) goja.Value {
	rt := p.vu.Runtime()

	// Convert the input Uint8Array to a Go []byte
	jsBuf := data.ToObject(rt)
	length := jsBuf.Get("length").ToInteger()
	goBuf := make([]byte, length)
	for i := int64(0); i < length; i++ {
		goBuf[i] = byte(jsBuf.Get(i).ToInteger())
	}

	reader := &MemoryFileReader{
		buf: bytes.NewBuffer(goBuf),
	}

	pr, err := reader.NewParquetReader()
	if err != nil {
		common.Throw(rt, err)
	}

	num := int(pr.GetNumRows())
	res := make([]map[string]interface{}, num)

	if err := pr.Read(&res); err != nil {
		common.Throw(rt, err)
	}

	pr.ReadStop()
	return rt.ToValue(res)
}

// MemoryFileReader implements the source.ParquetFile interface from parquet-go,
// wrapping a byte buffer so we can read from in-memory data
type MemoryFileReader struct {
	buf *bytes.Buffer
}

func (m *MemoryFileReader) Create(name string) (source.ParquetFile, error) {
	return nil, errors.New("not implemented")
}

func (m *MemoryFileReader) Open(name string) (source.ParquetFile, error) {
	return nil, errors.New("not implemented")
}

func (m *MemoryFileReader) Seek(offset int64, whence int) (int64, error) {
	return m.buf.Seek(offset, whence)
}

func (m *MemoryFileReader) Read(p []byte) (int, error) {
	return m.buf.Read(p)
}

func (m *MemoryFileReader) Write(p []byte) (int, error) {
	return 0, errors.New("write not supported")
}

func (m *MemoryFileReader) Close() error {
	return nil
}

func (m *MemoryFileReader) Name() string {
	return "memory"
}

func (m *MemoryFileReader) NewParquetReader() (*reader.ParquetReader, error) {
	return reader.NewParquetReader(m, nil, int64(4))
}
