package db

import (
	"github.com/tidwall/buntdb"
)

func InitDB() (*buntdb.DB, error) {
	return buntdb.Open(":memory:")
}
