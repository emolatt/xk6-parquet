package parquet

import (
	"bytes"
	"fmt"
	"github.com/emolatt/xk6-parquet/pkg/source"
	"github.com/xitongsys/parquet-go/source"
	"go.k6.io/k6/js/modules"
)

// MemoryFileReader is a custom file reader to read Parquet data from memory (e.g., Uint8Array)
type MemoryFileReader struct {
	buf []byte
}

// New creates a new MemoryFileReader from the byte array
func NewMemoryFileReader(data []byte) *MemoryFileReader {
	return &MemoryFileReader{buf: data}
}

// Read reads from the memory buffer
func (m *MemoryFileReader) Read(p []byte) (n int, err error) {
	copy(p, m.buf)
	return len(m.buf), nil
}

// Create is a placeholder method to implement the ParquetFile interface
func (m *MemoryFileReader) Create(path string) (source.ParquetFile, error) {
	return m, nil
}

// Write is a placeholder method to implement the ParquetFile interface
func (m *MemoryFileReader) Write(p []byte) (n int, err error) {
	// Not implemented
	return 0, fmt.Errorf("Write method not implemented")
}

// init registers the module with k6
func init() {
	modules.Register("k6/x/parquet", New)
}
