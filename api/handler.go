package api

import (
	"database/sql"
	"datastream/config"
	"datastream/logs"
	"datastream/models"
	"datastream/service"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"

	"html/template"
	"net/http"

	"github.com/joho/godotenv"
)

var tmpl *template.Template
var HomePage = template.Must(template.ParseFiles("/home/user/Desktop/datastream-project/data_stream_repo/templates/HomePage.html"))

func UploadPageHandler(w http.ResponseWriter, _ *http.Request) {
	var err error
	tmpl, err = template.ParseFiles("/home/user/Desktop/datastream-project/data_stream_repo/templates/HomePage.html")
	if err != nil {
		// Log the error using the logger
		logs.Logger.Error("Error", err)
	}
	err = tmpl.Execute(w, nil)

}

func UploadToKafka(w http.ResponseWriter, r *http.Request) {

	File, handler, err := r.FormFile("file")
	defer File.Close()
	if err != nil {
		renderHomePageWithError(w, "No file uploaded")
		return
	}

	// Checking if the file is a CSV
	if filepath.Ext(handler.Filename) != ".csv" {
		renderHomePageWithError(w, "Not a CSV file")
		return
	}

	filename := filepath.Join("/home/user/Documents/", handler.Filename)
	out, err := os.Create(filename)
	if err != nil {
		logs.Logger.Error("Error", err)

	}
	defer out.Close()

	// Copy the file content to the new file
	_, err = io.Copy(out, File)
	if err != nil {
		logs.Logger.Error("Error", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		logs.Logger.Error("Error", err)

	}
	defer file.Close()
	csvReader := csv.NewReader(file)

	// Create a CSV reader

	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}
	var batchSize = 100
	var batch []models.Contacts
	kafkaConnector, ContactsTopic, _, brokerList := OpenKafkaConnector()
	kafkaConnector, _, ContactActivityTopic, brokerList := OpenKafkaConnector()
	Db := OpenMysqlconnection()

	go SendContactsToSql(brokerList, ContactsTopic, Db)
	go SendContactActivityToSql(brokerList, ContactActivityTopic, Db)
	uniqueID := generateUniqueID()
	for idCounter := uniqueID; ; idCounter = generateUniqueID() {

		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break // Exit the loop when the end of the file is reached
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// Validate the fields
		if err := ValidateFields(record); err != nil {

			logs.Logger.Error("Invalid CSV format:", err)

		}

		contactStructInstance := models.Contacts{
			ID:      idCounter,
			Name:    record[0],
			Email:   record[1],
			Details: record[2],
		}

		mutex.Lock()
		batch = append(batch, contactStructInstance)
		if len(batch) >= batchSize {
			batchToSend := make([]models.Contacts, len(batch))
			copy(batchToSend, batch)
			wg.Add(1)
			go func(data []models.Contacts) {
				defer wg.Done()
				for _, contact := range data {
					dataTokafka := contact.ToCSV()
					fmt.Println(dataTokafka)
					datastring := []string{dataTokafka}
					err := service.SendToKafka(datastring, kafkaConnector, ContactsTopic)
					if err != nil {
						logs.Logger.Error("Error sending contacts to Kafka:", err)
					}
				}
				go generateActivitiesInBackground(data)

			}(batchToSend)
			wg.Wait()
			batch = nil // Reset batch to an empty slice after sending the data
		}
		mutex.Unlock()
	}

	// Process any remaining data in batch
	if len(batch) > 0 {
		wg.Add(1)
		go func(data []models.Contacts) {
			defer wg.Done()
			for _, contact := range data {
				dataTokafka := contact.ToCSV()
				datastring := []string{dataTokafka}
				err := service.SendToKafka(datastring, kafkaConnector, ContactsTopic)
				if err != nil {
					logs.Logger.Error("Error sending contacts to Kafka:", err)
				}
			}
			generateActivitiesInBackground(data)
		}(batch)
	}

	wg.Wait()
}

func generateActivitiesInBackground(data []models.Contacts) {
	// activityData := []models.ContactActivity{}
	kafkaConnector, _, ContactActivityTopic, _ := OpenKafkaConnector()

	var dataStrings []string

	for _, row := range data {
		activities := CallActivity(row.ID)
		for _, activity := range activities {
			dataToKafka := activity.ToCSV()
			dataStrings = append(dataStrings, dataToKafka)
		}
	}

	if len(dataStrings) > 0 {
		err := service.SendToKafka(dataStrings, kafkaConnector, ContactActivityTopic)
		if err != nil {
			logs.Logger.Error("Error sending contact activities to Kafka:", err)
		}
	}
}
func OpenKafkaConnector() (*config.KafkaConnector, string, string, string) {
	err := godotenv.Load("/home/user/Desktop/datastream-project/data_stream_repo/.env")
	if err != nil {
		logs.Logger.Error("Error loading .env file:", err)
	}

	brokerList := os.Getenv("BROKER_LIST")
	ContactsTopic := os.Getenv("KafkaTopic_1")
	ContactActivityTopic := os.Getenv("KafkaTopic_2")
	// Create Kafka config
	kafkaConfig := config.KafkaConfig{
		Broker: brokerList,
	}

	// Create Kafka connector
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig)
	if err != nil {
		panic(err)
	}

	return kafkaConnector, ContactsTopic, ContactActivityTopic, brokerList
}
func OpenMysqlconnection() *sql.DB {

	db, err := config.DBMysql()
	if err != nil {
		logs.Logger.Error("fatal error", err)
	}

	return db
}

// generateUniqueID generates a unique ID based on the current timestamp and a random number.
func generateUniqueID() int {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	randomNumber := rand.Intn(1000)
	uniqueID := int(timestamp) + randomNumber
	// Reduce the size to 6 digits
	uniqueID = uniqueID % 1000000
	return uniqueID
}
func renderHomePageWithError(w http.ResponseWriter, errorMessage string) {

	data := struct {
		Error string
	}{Error: errorMessage}

	if err := HomePage.Execute(w, data); err != nil {

		http.Error(w, errorMessage, http.StatusBadRequest)

	}

}

// ValidateFields validates the header fields of a CSV.
func ValidateFields(record []string) error {
	if len(record) < 3 {
		return errors.New("Invalid CSV format - insufficient fields")
	}

	if !isString(record[0]) {

		return errors.New("Invalid CSV format - name error")
	} else if !isEmail(record[1]) {

		return errors.New("Invalid CSV format - email error")
	} else if !isJson(record[2]) {

		return errors.New("Invalid CSV format - JSON error")
	}
	return nil
}

// check if a string contains only letters
func isString(data string) bool {
	stringRegex := regexp.MustCompile(`^[a-zA-Z0-9]+$`)
	return stringRegex.MatchString(data)
}

// check if a string is a valid email
func isEmail(data string) bool {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(data)
}

func isJson(data string) bool {
	// Check if the length of the input string is greater than 3 characters

	if len(data) <= 3 {

		return false

	}

	var js map[string]interface{}

	if err := json.Unmarshal([]byte(data), &js); err != nil {

		return false

	}

	// Check if the JSON has the required keys and values

	requiredKeys := []string{"dob", "country", "city"}

	for _, key := range requiredKeys {

		if _, exists := js[key]; !exists {

			return false

		}

	}

	return true

}

func GetDataFromClickHouse(w http.ResponseWriter, r *http.Request) {

	tmpl, err := template.ParseFiles("/home/user/Desktop/datastream-project/data_stream_repo/templates/Output.html")

	if err != nil {

		logs.Logger.Error("ERROR:", err)

		return

	}

	ClickhouseDb, err := config.DBClickhouse() // Connect to clickhouse
	if err != nil {

		logs.Logger.Error("Error", err)

	}

	defer ClickhouseDb.Close()
	query := "SELECT CampaignID,abusive FROM safana_campaign.contact_activity_summary_MV_ContactActivity ORDER BY abusive DESC LIMIT 5;"

	rows := service.ClickhouseDb(query, ClickhouseDb)

	var results []models.ResultRow

	for rows.Next() {

		var row models.ResultRow

		err := rows.Scan(&row.CampaignId, &row.Abusive)

		if err != nil {

			logs.Logger.Error("Error", err)

		}

		results = append(results, row)

	}
	err = tmpl.Execute(w, results)

	if err != nil {

		logs.Logger.Error("Error", err)

	}

	// Parse and execute the "results.html" template with the result.

}
