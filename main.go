package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"go.k6.io/k6/js/modules"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// ---- MemoryFileReader definíció ----
type MemoryFileReader struct {
	mu     sync.Mutex
	Reader *bytes.Reader
	buf    *bytes.Buffer
}

func (m *MemoryFileReader) Create(name string) (source.ParquetFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf = new(bytes.Buffer)
	m.Reader = nil
	return m, nil
}

func (m *MemoryFileReader) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buf == nil {
		return 0, errors.New("buffer not initialized")
	}
	return m.buf.Write(p)
}

func (m *MemoryFileReader) Open(name string) (source.ParquetFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buf == nil {
		return nil, errors.New("buffer not initialized")
	}
	m.Reader = bytes.NewReader(m.buf.Bytes())
	return m, nil
}

func (m *MemoryFileReader) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Reader == nil {
		return 0, errors.New("reader not initialized")
	}
	return m.Reader.Read(p)
}

func (m *MemoryFileReader) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Reader == nil {
		return 0, errors.New("reader not initialized")
	}
	return m.Reader.Seek(offset, whence)
}

func (m *MemoryFileReader) Close() error {
	return nil
}

func (m *MemoryFileReader) Name() string {
	return "memory"
}

// ---- JS modul ----

type Parquet struct{}

func (p *Parquet) ReadParquetFromBytes(_ context.Context, data []byte) ([]map[string]interface{}, error) {
	mem := &MemoryFileReader{}
	if _, err := mem.Create("in-memory"); err != nil {
		return nil, err
	}

	if _, err := mem.Write(data); err != nil {
		return nil, err
	}

	if _, err := mem.Open("in-memory"); err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(mem, nil, 1)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	res := make([]map[string]interface{}, 0, num)

	for i := 0; i < num; i += 10 {
		count := 10
		if i+10 > num {
			count = num - i
		}
		rows := make([]map[string]interface{}, 0)
		if err := pr.Read(&rows); err != nil && err != io.EOF {
			return nil, err
		}
		res = append(res, rows...)
	}

	return res, nil
}

// Module export
func init() {
	modules.Register("k6/x/parquet", new(Parquet))
}
