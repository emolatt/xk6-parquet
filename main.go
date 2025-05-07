package modules

import (
    "fmt"
    "go.k6.io/k6/js/modules"
    "github.com/xitongsys/parquet-go/source"
    "github.com/xitongsys/parquet-go/reader"
)

// ParquetModule represents the Parquet module
type ParquetModule struct{}

// MemoryFileReader implements the ParquetFile interface for reading from memory
type MemoryFileReader struct {
    data []byte
}

func (m *MemoryFileReader) Read(b []byte) (n int, err error) {
    return copy(b, m.data), nil
}

func (m *MemoryFileReader) Close() error {
    return nil
}

// Implement Create method as a dummy since we're not writing files
func (m *MemoryFileReader) Create(path string) error {
    return nil // no actual file creation needed
}

func (m *MemoryFileReader) Open(name string) (source.ParquetFile, error) {
    return m, nil
}

func (m *MemoryFileReader) Seek(offset int64, whence int) (int64, error) {
    return 0, fmt.Errorf("seek not supported")
}

// ReadParquetFromByteArray reads parquet data from byte array and returns it as a map
func (m *ParquetModule) ReadParquetFromByteArray(jsContext context.Context, data []byte) (map[string]interface{}, error) {
    // Create a new memory reader for the data
    parquetReader, err := reader.NewParquetReader(&MemoryFileReader{data: data}, nil, 1)
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
func (m *ParquetModule) Exports() modules.Exports {
    return modules.Exports{
        "readParquetFromByteArray": m.ReadParquetFromByteArray,
    }
}

// NewModuleInstance returns the instance of the module
func (m *ParquetModule) NewModuleInstance(vu modules.VU) (modules.Instance, error) {
    return m, nil
}

// New creates a new ParquetModule instance
func New() modules.Module {
    return &ParquetModule{}
}

func init() {
    modules.Register("k6/xk6-parquet", New)
}
