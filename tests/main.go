package main

import (
	"database/sql"
	"fmt"

	_ "github.com/demouth/mysqldriver"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// using driver: mysqldriver

	db2, err := sql.Open("mysqldriver", "user:password@tcp(localhost:9910)/test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		panic(err.Error())
	}
	defer db2.Close()
	fmt.Println("mysqldriver: Ping", db2.Ping())
	var rows *sql.Rows
	rows, err = db2.Query("SHOW DATABASES WHERE `Database` LIKE ?", `%schema`)
	// rows, err = db2.Query("SHOW DATABASES")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			panic(err.Error())
		}
		fmt.Println("mysqldriver:", database)
	}

	// using driver: mysql

	db, err := sql.Open("mysql", "user:password@tcp(localhost:9911)/test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	fmt.Println("mysql: Ping", db.Ping())
	rows, err = db.Query("SHOW DATABASES WHERE `Database` LIKE ?", `%schema`)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			panic(err.Error())
		}
		fmt.Println("mysql:", database)
	}
}
