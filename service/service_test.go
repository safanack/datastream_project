package service

import (
	"database/sql"
	"datastream/config"
	"fmt"

	"datastream/models"

	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSendToKafka(t *testing.T) {

	topic := "test-topic1"
	brokerList := "localhost:9092"
	// brokerList := "localhost:9090"
	kafkaConfig := config.KafkaConfig{
		Broker: brokerList,
	}

	// Create Kafka connector
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig)
	if err != nil {
		panic(err)
	}
	defer kafkaConnector.Close()

	// Define test contacts data
	contacts := []models.Contacts{{
		ID:      1,
		Name:    "John Doe",
		Email:   "johndoe@example.com",
		Details: "Some details about John Doe",
	}}

	var contactStrings []string
	for _, contact := range contacts {
		contactStr := fmt.Sprintf("%d,%s,%s,%s", contact.ID, contact.Name, contact.Email, contact.Details)
		contactStrings = append(contactStrings, contactStr)
	}

	// Use contactStrings in SendToKafka function
	err = SendToKafka(contactStrings, kafkaConnector, topic)

	if err != nil {
		t.Errorf("Error sending contacts to Kafka: %v", err)
	}

	if err != nil {
		t.Errorf("Error sending contacts to Kafka: %v", err)
	}
}

type DatabaseConnector interface {
	ConnectDatabase(string) (*sql.DB, error)
}

type MockDatabaseConnector struct {
	mock.Mock
}

func (m *MockDatabaseConnector) ConnectDatabase(databaseName string) (*sql.DB, error) {
	args := m.Called(databaseName)
	return args.Get(0).(*sql.DB), args.Error(1)
}
func TestClickhouseDb(t *testing.T) {
	// Create a real in-memory SQLite database for testing.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create a database connection: %v", err)
	}
	defer db.Close()

	// Create the required 'your_table' for the test in the SQLite database.
	_, err = db.Exec(`
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}
	// Insert sample data into the table.
	_, err = db.Exec(`INSERT INTO test_table (id,name) VALUES (1,"rahul"), (2,"Alice"), (3,"Bob")`)
	if err != nil {
		t.Fatalf("Failed to insert data into the table: %v", err)
	}
	// Define the expected query.
	expectedQuery := `SELECT * FROM test_table;`

	// Call the function with the query string and the database connection.
	rows := ClickhouseDb(expectedQuery, db)

	// Assert that the function returned the expected values.
	assert.NoError(t, err)
	assert.NotNil(t, rows)

	// // Test with a wrong query.
	// wrongQuery := `SELECT * FROM non_existent_table;`
	// rows = ClickhouseDb(wrongQuery, db)
	// // Assert that an error is returned when a wrong query is executed.
	// assert.Error(t, err)
}
func TestMysqlDB(t *testing.T) {
	// Create a real in-memory SQLite database for testing.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create a database connection: %v", err)
	}
	defer db.Close()

	// Create the required 'your_table' for the test in the SQLite database.
	_, err = db.Exec(`
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}
	// Insert sample data into the table.
	_, err = db.Exec(`INSERT INTO test_table (id,name) VALUES (1,"rahul"), (2,"Alice"), (3,"Bob")`)
	if err != nil {
		t.Fatalf("Failed to insert data into the table: %v", err)
	}
	// Define the expected query.
	expectedQuery := `SELECT * FROM test_table;`

	// Call the function with the query string and the database connection.
	err = MysqlDB(expectedQuery, db)

	// Assert that the function returned the expected values.
	assert.NoError(t, err)

}
