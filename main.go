package modules

import (
    "bytes"
    "fmt"
    "go.k6.io/k6/js/modules"
    "github.com/xitongsys/parquet-go/source"
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

// Create method should accept a string parameter (file path or name) and return a ParquetFile.
func (m *MemoryFileReader) Create(string) (source.ParquetFile, error) {
    return m, nil // Return itself as a valid ParquetFile
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
        // Ensure that the row is of type map[string]interface{}
        if row, ok := rows[0].(*map[string]interface{}); ok {
            for key, value := range *row {
                result[key] = value
            }
        } else {
            return nil, fmt.Errorf("unexpected row type")
        }
    }

    return result, nil
}

// Exports returns the functions that should be available in JS
func (m *ParquetModule) Exports() map[string]interface{} {
    return map[string]interface{}{
        "ReadParquetFromByteArray": m.ReadParquetFromByteArray,
    }
}

// NewModuleInstance returns the instance of the module
func (m *ParquetModule) NewModuleInstance(vu modules.VU) (modules.Instance, error) {
    return m, nil
}

func New() modules.Module {
    return &ParquetModule{}
}

func init() {
    modules.Register("k6/xk6-parquet", New)
}
