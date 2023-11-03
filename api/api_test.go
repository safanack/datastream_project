package api

import (
	"bytes"
	"database/sql"
	"datastream/models"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/mock"
)

func TestUploadPageHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/upload", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(UploadPageHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

}

func TestUploadToKafka(t *testing.T) {
	// Test Case 1: No file uploaded
	req1 := httptest.NewRequest("POST", "/upload", nil)
	w1 := httptest.NewRecorder()
	UploadToKafka(w1, req1)
	resp1 := w1.Result()
	if resp1.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %v but got %v", http.StatusOK, resp1.StatusCode)
	}

	// Test Case 2: Not a CSV file
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, err = part.Write([]byte("some dummy content"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	req2 := httptest.NewRequest("POST", "/upload", body)
	req2.Header.Add("Content-Type", writer.FormDataContentType())
	w2 := httptest.NewRecorder()
	UploadToKafka(w2, req2)
	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %v but got %v", http.StatusOK, resp2.StatusCode)
	}

}

func TestUploadToKafkaHandler(t *testing.T) {
	// Mock the HTTP request
	var jsonStr = []byte(`{"file": "sampleFileContent"}`)
	req, err := http.NewRequest("POST", "/upload", bytes.NewBuffer(jsonStr))
	if err != nil {
		t.Fatal(err)
	}

	// Test Case 1: No file uploaded
	w1 := httptest.NewRecorder()
	UploadToKafka(w1, req)
	resp1 := w1.Result()
	if resp1.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %v but got %v", http.StatusOK, resp1.StatusCode)
	}
	// Create a ResponseRecorder to record the response
	responcerecorder := httptest.NewRecorder()

	// Call the handler function with the mock request and response
	UploadToKafka(responcerecorder, req)

	// Check the status code is what we expect
	if status := responcerecorder.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

}

func TestGenerateUniqueID(t *testing.T) {
	// Set a fixed seed for the random number generator for reproducibility
	rand.Seed(time.Now().UnixNano())

	// Call the function to generate the unique ID
	uniqueID := generateUniqueID()

	// Check if the generated ID is within the expected range
	if uniqueID < 0 || uniqueID > 999999 {
		t.Errorf("Generated unique ID is not within the expected range")
	}
}
func TestOpenKafkaConnector(t *testing.T) {
	// Load environment variables from .env file
	err := godotenv.Load("/home/user/Desktop/datastream-project/data_stream_repo/.env")
	if err != nil {
		t.Error("Error loading .env file:", err)
	}

	expectedBrokerList := os.Getenv("BROKER_LIST")
	expectedContactsTopic := os.Getenv("KafkaTopic_1")
	expectedContactActivityTopic := os.Getenv("KafkaTopic_2")

	kafkaConnector, ContactsTopic, ContactActivityTopic, brokerList := OpenKafkaConnector()

	if kafkaConnector == nil {
		t.Error("Expected a non-nil kafkaConnector but got nil")
	}

	if ContactsTopic != expectedContactsTopic {
		t.Errorf("Expected ContactsTopic %s but got %s", expectedContactsTopic, ContactsTopic)
	}

	if ContactActivityTopic != expectedContactActivityTopic {
		t.Errorf("Expected ContactActivityTopic %s but got %s", expectedContactActivityTopic, ContactActivityTopic)
	}

	if brokerList != expectedBrokerList {
		t.Errorf("Expected brokerList %s but got %s", expectedBrokerList, brokerList)
	}
}
func TestValidateFields(t *testing.T) {
	validRecord := []string{"JohnDoe", "john@example.com", `{"country":"india","city":"kerala","dob":"29-09-2001"}`}
	invalidRecordInsufficientFields := []string{"John", "johndoe@example.com"}
	invalidRecordName := []string{"", "john@example.com", `{"country":"india","city":"kerala","dob":"29-09-2001"}`}
	invalidRecordEmail := []string{"John", "notanemail", `{"country":"india","city":"kerala","dob":"29-09-2001"}`}
	invalidRecordJSON := []string{"John", "johndoe@example.com", "notjson"}

	err := ValidateFields(validRecord)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	err = ValidateFields(invalidRecordInsufficientFields)
	if err == nil || err.Error() != "Invalid CSV format - insufficient fields" {
		t.Errorf("Expected 'Invalid CSV format - insufficient fields', but got %v", err)
	}

	err = ValidateFields(invalidRecordName)
	if err == nil || err.Error() != "Invalid CSV format - name error" {
		t.Errorf("Expected 'Invalid CSV format - name error', but got %v", err)
	}

	err = ValidateFields(invalidRecordEmail)
	if err == nil || err.Error() != "Invalid CSV format - email error" {
		t.Errorf("Expected 'Invalid CSV format - email error', but got %v", err)
	}

	err = ValidateFields(invalidRecordJSON)
	if err == nil || err.Error() != "Invalid CSV format - JSON error" {
		t.Errorf("Expected 'Invalid CSV format - JSON error', but got %v", err)
	}
}

type MockService struct {
	mock.Mock
}

func (m *MockService) ClickhouseDb(query string) *sql.Rows {
	args := m.Called(query)
	return args.Get(0).(*sql.Rows)
}

func TestGetDataFromClickHouse(t *testing.T) {
	// Create a mock service
	mockService := new(MockService)

	// Define the expected rows
	expectedRows := &sql.Rows{} // Define your expected rows here

	// Mock the ClickhouseDb function
	mockService.On("ClickhouseDb", mock.Anything).Return(expectedRows)

	req, err := http.NewRequest("GET", "/GetResult", nil)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		GetDataFromClickHouse(w, r)
	})

	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body to make sure it contains the expected data.
	expected := "Expected data" // Add the expected data from the template here.
	if recorder.Body.String() != expected {
		t.Errorf("Handler returned unexpected body: got %v want %v", recorder.Body.String(), expected)
	}

	// Assert that the ClickhouseDb function was called once
	mockService.AssertExpectations(t)
}

// Define a mock struct
type MockActivity struct {
	mock.Mock
}

// Define a mocked method for the CallActivity function
func (m *MockActivity) CallActivity(id int) []models.ContactActivity {
	args := m.Called(id)
	return args.Get(0).([]models.ContactActivity)
}

// Test the generateActivitiesInBackground function
func TestGenerateActivitiesInBackground(t *testing.T) {
	mockActivity := new(MockActivity)

	// Define the expected return value for the mock
	expectedActivity := models.ContactActivity{
		ContactID:    65432,
		CampaignID:   1,
		ActivityType: 1,
		ActivityDate: time.Now(),
	}

	mockActivity.On("CallActivity", mock.AnythingOfType("int")).Return(expectedActivity)

	// Create sample data to pass to the function
	sampleData := []models.Contacts{
		{
			ID:      1,
			Name:    "safa",
			Email:   "safa@gmail.com",
			Details: "some details",
		},
	}

	generateActivitiesInBackground(sampleData)

	// Assert that the function was called with the expected argument
	mockActivity.AssertCalled(t, "CallActivity", mock.AnythingOfType("int"))
}
func TestCallActivity(t *testing.T) {

	result := CallActivity(1)

	if result == nil {
		t.Errorf("Test failed, expected non-nil result, got nil")
	} else {
		t.Logf("Test passed successfully")
	}
}
func BenchmarkCallActivity(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Replace the argument with the appropriate ID for the function
		CallActivity(1)
	}
}
func TestSendContactsToSql(t *testing.T) {
	topic := "test-topic1"
	brokerList := "localhost:9092"

	// Create a mock DB connection for testing purposes.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create a database connection: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`
	CREATE TABLE safa_contact (
		ID int NOT NULL,
		Name varchar(255) NOT NULL,
		Email varchar(255) NOT NULL,
		Details varchar(300) NOT NULL
	  )
    `)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}

	expectedQuery := "INSERT INTO `Contacts`(ID, Name, Email, Details) VALUES (1, 'safa', 'safa@gmail.com', 'some details')"

	// Call the function for testing.
	SendContactsToSql(brokerList, topic, db)

	// Check the expected outcome against the query executed.
	if strings.Contains(expectedQuery, "error") {
		t.Errorf("Test failed: Expected query contains error.")
	}

	// Add additional checks if required.
}
func BenchmarkSendContactsToSql(b *testing.B) {
	brokerList := "localhost:9092"
	topic := "test-topic1"
	// Create a mock DB connection for testing purposes.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("Failed to create a database connection: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`
	CREATE TABLE safa_contact (
		ID int NOT NULL,
		Name varchar(255) NOT NULL,
		Email varchar(255) NOT NULL,
		Details varchar(300) NOT NULL
	  )
    `)
	if err != nil {
		b.Fatalf("Failed to create the table: %v", err)
	}

	// Run the function b.N times
	for n := 0; n < b.N; n++ {
		SendContactsToSql(brokerList, topic, db)
	}
}
