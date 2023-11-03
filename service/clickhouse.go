package service

import (
	"database/sql"
	"datastream/logs"
)

func ClickhouseDb(query string, db *sql.DB) *sql.Rows {

	rows, err := db.Query(query)

	if err != nil {

		logs.Logger.Error("Error", err)

	}

	return rows

}
