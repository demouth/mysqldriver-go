package main

import (
	"testing"

	"github.com/demouth/gormysql"
	_ "github.com/demouth/mysqldriver"
)

type User struct {
	Id   int64
	Name string
}

func TestORM(t *testing.T) {
	db, err := gormysql.OpenWithDriver("mysqldriver", "user:password@tcp(localhost:9910)/test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		t.Fatalf("no error should happen when connect database, but got %+v", err)
	}

	err = db.Exec("DROP TABLE IF EXISTS users;").Error
	if err != nil {
		t.Fatalf("got error when try to delete table users, %+v", err)
	}

	orm := db.CreateTable(&User{})
	if orm.Error != nil {
		t.Fatalf("no error should happen when create table, but got %+v", orm.Error)
	}

	orm = db.Save(&User{Name: "Alice"})
	if orm.Error != nil {
		t.Fatalf("no error should happen when save, but got %+v", orm.Error)
	}

	var u User
	orm = db.Find(&u)
	if orm.Error != nil {
		t.Fatalf("no error should happen when find, but got %+v", orm.Error)
	}

	if u.Name != "Alice" {
		t.Errorf("expected name to be 'Alice', but got '%s'", u.Name)
	}
}
