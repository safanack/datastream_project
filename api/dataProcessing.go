package api

import (
	"database/sql"
	"datastream/logs"
	"datastream/service"
	"fmt"
	"strconv"
	"strings"
)

func SendContactsToSql(brokerList string, topic string, Db *sql.DB) {

	messageChannel := make(chan string, 1000)
	go service.ConsumeMessages(brokerList, topic, messageChannel)

	for messages := range messageChannel {
		fmt.Println(messages)
		go func(messages string) {
			Values := strings.SplitN(messages, ",", 4)

			if len(Values) >= 4 {

				field1 := Values[0]
				convertedField1, err := strconv.Atoi(field1)
				if err != nil {
					// Handle the error if the conversion fails.
					fmt.Println("Error converting string to integer:", err)
					return
				}

				field2 := Values[1]

				field3 := Values[2]

				field4 := Values[3]

				query := fmt.Sprintf("INSERT INTO `safa_contact`(ID, Name, Email, Details) VALUES (%d, '%s', '%s', '%s')",
					convertedField1, field2, field3, field4)
				fmt.Println(query)

				err = service.MysqlDB(query, Db)
				messages = ""
				if err != nil {

					logs.Logger.Error("error in exicuting query: %v\n", err)

				}

			}
		}(messages)

	}
}

func SendContactActivityToSql(brokerList string, topic string, Db *sql.DB) {

	messageChannel := make(chan string, 1000)
	go service.ConsumeMessages(brokerList, topic, messageChannel)
	for messages := range messageChannel {
		go func(messages string) {
			Values := strings.SplitN(messages, ",", 4)

			if len(Values) >= 4 {

				field1 := Values[0]

				field2 := Values[1]

				field3 := Values[2]

				field4 := Values[3]
				Field1, err := strconv.Atoi(field1)
				Field2, err := strconv.Atoi(field2)
				Field3, err := strconv.Atoi(field3)
				if err != nil {
					// Handle the error if the conversion fails.
					logs.Logger.Error("Error converting string to integer:", err)
					return
				}

				query := fmt.Sprintf("INSERT INTO `safa_contact_activity`(ContactsID, CampaignID,ActivityType , ActivityDate) VALUES (%d, %d, %d, '%s')", Field1, Field2, Field3, field4)

				err = service.MysqlDB(query, Db)
				if err != nil {

					logs.Logger.Error("error in exicuting query: %v\n", err)

				}
			}
		}(messages)
	}

}
