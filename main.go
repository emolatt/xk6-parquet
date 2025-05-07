package modules

import (
	"bytes"
	"context"
	"fmt"
	"go.k6.io/k6/js/modules"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetModule struct{}

// MemoryFileReader implements the ParquetFile interface
type MemoryFileReader struct {
	data *bytes.Reader
}

func (m *MemoryFileReader) Read(b []byte) (n int, err error) {
	return m.data.Read(b)
}

func (m *MemoryFileReader) Close() error {
	return nil
}

func (m *MemoryFileReader) Create() error {
	// MemoryFileReader does not need to actually create anything.
	return nil
}

// ReadParquetFromByteArray reads a Parquet file from a byte array and returns a map representation.
func (m *ParquetModule) ReadParquetFromByteArray(jsContext context.Context, data []byte) (map[string]interface{}, error) {
	// Create a new memory reader for the data
	parquetReader, err := reader.NewParquetReader(&MemoryFileReader{data: bytes.NewReader(data)}, nil, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %v", err)
	}

	// Read a row (you can adjust this to your use case)
	rows, err := parquetReader.ReadByNumber(1)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet rows: %v", err)
	}

	// For simplicity, let's return the first row as a JSON map
	result := make(map[string]interface{})
	if len(rows) > 0 {
		for key, value := range rows[0].(*map[string]interface{}) {
			result[key] = value
		}
	}

	return result, nil
}

// ---- JS module ----
func (m *ParquetModule) NewModuleInstance(ctx context.Context, config map[string]interface{}) (modules.Instance, error) {
	return m, nil
}

func (m *ParquetModule) Exports() map[string]interface{} {
	return map[string]interface{}{
		"ReadParquetFromByteArray": m.ReadParquetFromByteArray,
	}
}

func New() modules.Module {
	return &ParquetModule{}
}

func init() {
	modules.Register("k6/xk6-parquet", New)
}
