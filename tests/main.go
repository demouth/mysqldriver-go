package main

import (
	"database/sql"

	_ "github.com/demouth/mysqldriver"
	// _ "github.com/go-sql-driver/mysql"
)

func main() {
	db2, err := sql.Open("mysqldriver", "gorm:gorm@tcp(localhost:9910)/gorm?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		panic(err.Error())
	}
	defer db2.Close()
	println("Ping", db2.Ping())

	/*
		db, err := sql.Open("mysql", "gorm:gorm@tcp(localhost:9910)/gorm?charset=utf8&parseTime=True&loc=Local")
		if err != nil {
			panic(err.Error())
		}
		defer db.Close()

		println("Ping", db.Ping())
		r, e := db.Exec("SHOW DATABASES")
		println(r, e)
		rows, err := db.Query("SHOW DATABASES")
		if err != nil {
			panic(err.Error())
		}
		defer rows.Close()
		for rows.Next() {
			var database string
			if err := rows.Scan(&database); err != nil {
				panic(err.Error())
			}
			fmt.Println(database)
		}
	*/
}
