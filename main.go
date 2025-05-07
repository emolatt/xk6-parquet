package parquet

import (
    "context"
    "go.k6.io/k6/js/modules"
)

func init() {
    modules.Register("k6/x/parquet", New())
}

type RootModule struct{}

func New() modules.Module {
    return &RootModule{}
}

func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
    return &ParquetReader{vu: vu}
}

type ParquetReader struct {
    vu modules.VU
}

// ide jön majd a fájlból és memóriából olvasó kód

