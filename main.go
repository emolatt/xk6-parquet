package main

import (
	"context"
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.k6.io/k6/js/modules"
)

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

func ReadParquetFromByteArray(_ context.Context, data []byte) (map[string]interface{}, error) {
	memReader := &MemoryFileReader{data: data}
	pr, err := reader.NewParquetReader(memReader, nil, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer pr.ReadStop()

	rows, err := pr.ReadByNumber(1)
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	result := make(map[string]interface{})
	if len(rows) > 0 {
		if row, ok := rows[0].(map[string]interface{}); ok {
			for k, v := range row {
				result[k] = v
			}
		}
	}

	return result, nil
}

func init() {
	modules.Register("k6/x/xk6-parquet", modules.Module{
		Exports: modules.Exports{
			Named: map[string]interface{}{
				"readParquetFromByteArray": ReadParquetFromByteArray,
			},
		},
	})
}
