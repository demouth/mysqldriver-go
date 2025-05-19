module github.com/demouth/mysqldriver/tests

go 1.24.1

replace github.com/demouth/mysqldriver => ../../

require (
	github.com/demouth/mysqldriver v0.0.0-00010101000000-000000000000
	github.com/go-sql-driver/mysql v1.9.2
)

require filippo.io/edwards25519 v1.1.0 // indirect
