package service

import (
	"database/sql"
	"datastream/logs"
	"fmt"
)

func MysqlDB(query string, db *sql.DB) error {
	if db != nil {
		responce, err := db.Exec(query)
		if responce != nil {
			fmt.Printf("mysql error responce %v\n", responce)
		}
		if err != nil {
			fmt.Printf("Failed to insert data into MySQL: %v\n", err)

		} else if err == nil {

			logs.Logger.Info("Inserted data into MySQL successfully")
		}
	}
	return nil
}
