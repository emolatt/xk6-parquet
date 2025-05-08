package modules

import (
    "context"
    "fmt"
    "io"

    "github.com/xitongsys/parquet-go/reader"
    "github.com/xitongsys/parquet-go/source"
    "go.k6.io/k6/js/modules"
)

type ParquetModule struct{}

type ParquetInstance struct {
    modules.Instance
}

type MemoryFileReader struct {
    data []byte
    pos  int64
}

func (m *MemoryFileReader) Read(b []byte) (int, error) {
    if m.pos >= int64(len(m.data)) {
        return 0, io.EOF
    }
    n := copy(b, m.data[m.pos:])
    m.pos += int64(n)
    return n, nil
}

func (m *MemoryFileReader) Write(p []byte) (int, error) {
    return 0, fmt.Errorf("write not supported on MemoryFileReader")
}

func (m *MemoryFileReader) Seek(offset int64, whence int) (int64, error) {
    var newPos int64
    switch whence {
    case io.SeekStart:
        newPos = offset
    case io.SeekCurrent:
        newPos = m.pos + offset
    case io.SeekEnd:
        newPos = int64(len(m.data)) + offset
    default:
        return 0, fmt.Errorf("invalid whence: %d", whence)
    }
    if newPos < 0 || newPos > int64(len(m.data)) {
        return 0, fmt.Errorf("invalid seek position")
    }
    m.pos = newPos
    return m.pos, nil
}

func (m *MemoryFileReader) Close() error {
    return nil
}

func (m *MemoryFileReader) Open(name string) (source.ParquetFile, error) {
    return m, nil
}

func (m *MemoryFileReader) Create(name string) (source.ParquetFile, error) {
    return m, nil
}

// Exportált JS-függvény
func (p *ParquetInstance) ReadParquetFromByteArray(ctx context.Context, data []byte) (map[string]interface{}, error) {
    memReader := &MemoryFileReader{data: data}
    parquetReader, err := reader.NewParquetReader(memReader, nil, 1)
    if err != nil {
        return nil, fmt.Errorf("failed to create parquet reader: %v", err)
    }
    defer parquetReader.ReadStop()

    rows, err := parquetReader.ReadByNumber(1)
    if err != nil {
        return nil, fmt.Errorf("failed to read parquet rows: %v", err)
    }

    result := make(map[string]interface{})
    if len(rows) > 0 {
        if row, ok := rows[0].(map[string]interface{}); ok {
            for k, v := range row {
                result[k] = v
            }
        } else {
            return nil, fmt.Errorf("unexpected row type")
        }
    }

    return result, nil
}

func (m *ParquetModule) NewModuleInstance(vu modules.VU) modules.Instance {
    return &ParquetInstance{}
}

func (m *ParquetModule) Exports() modules.Exports {
    return modules.Exports{
        Default: m,
        Named:   map[string]interface{}{},
    }
}

func New() modules.Module {
    return &ParquetModule{}
}

func init() {
    modules.Register("k6/x/xk6-parquet", New)
}
