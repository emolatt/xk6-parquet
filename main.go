package modules

import (
	"bytes"
	"context"
	"fmt"

	"go.k6.io/k6/js/modules"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetModule struct{}

// ReadParquetFromByteArray reads a Parquet file from a byte array and returns a map representation.
func (m *ParquetModule) ReadParquetFromByteArray(jsContext context.Context, data []byte) (map[string]interface{}, error) {
	// Create a new memory reader for the data
	parquetReader, err := reader.NewParquetReader(bytes.NewReader(data), nil, 1)
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
	for _, field := range rows[0].(*map[string]interface{}) {
		result[field] = field
	}

	return result, nil
}

// ---- JS module ----
func New() modules.Module {
	return &ParquetModule{}
}

func init() {
	modules.Register("k6/xk6-parquet", New)
}
