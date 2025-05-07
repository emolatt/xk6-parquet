package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"errors"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.k6.io/k6/js/modules"
)

// Register the extension with the k6 runtime
func init() {
	modules.Register("k6/x/parquet", New())
}

// RootModule implements the k6 modules.Module interface
type RootModule struct{}

// New returns a new instance of the module
func New() modules.Module {
	return &RootModule{}
}

// NewModuleInstance is called for each VU (virtual user)
func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ParquetReader{vu: vu}
}

// ParquetReader represents the module instance for a VU
type ParquetReader struct {
	vu modules.VU
}

type MemoryFileReader struct {
	buf *bytes.Buffer
}

// Exports defines the JavaScript API surface
func (pr *ParquetReader) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"readBuffer": pr.ReadBuffer,
		},
	}
}

// ReadBuffer reads Parquet data from a byte buffer (e.g. Uint8Array from HTTP response)
func (pr *ParquetReader) ReadBuffer(buf []byte, num int) ([]map[string]interface{}, error) {
	fr := NewMemoryFileReader(buf)

	prdr, err := reader.NewParquetReader(fr, nil, int64(num))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer prdr.ReadStop()

	raw, err := prdr.ReadByNumber(num)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(raw))
	for _, record := range raw {
		b, _ := json.Marshal(record)
		var m map[string]interface{}
		_ = json.Unmarshal(b, &m)
		result = append(result, m)
	}

	return result, nil
}

// NewMemoryFileReader creates a new in-memory reader for Parquet data
func NewMemoryFileReader(data []byte) source.ParquetFile {
	return &MemoryFileReader{
		Reader: bytes.NewReader(data),
	}
}

func (r *MemoryFileReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Reader.Seek(offset, whence)
}

func (r *MemoryFileReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Reader.Read(p)
}

func (r *MemoryFileReader) Close() error {
	return nil
}

func (m *MemoryFileReader) Open(name string) (source.ParquetFile, error) {
	// Mivel csak egy memória buffered van, ignoráljuk a `name` paramétert
	return m, nil
}

func (m *MemoryFileReader) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}
