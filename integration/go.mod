module github.com/tobibamidele/work/integration

go 1.22

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/tobibamidele/lathe v0.4.0
	github.com/tobibamidele/validate v0.0.0-20261008115932-920c462609fa
	github.com/tobibamidele/work v0.0.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
)

replace github.com/tobibamidele/work => ../
