module github.com/emolatt/xk6-parquet

go 1.24.2

require (
    go.k6.io/k6 v0.54.0
    github.com/xitongsys/parquet-go v1.6.3
)

replace go.k6.io/k6 => github.com/grafana/k6 v0.54.0
