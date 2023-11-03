package config

import (
	"database/sql"
	"datastream/logs"
	"errors"
	"fmt"
	"log"
	"os"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type DBConnector interface {
	Connect() error
	Close() error
}

// MySQL connection configuration.
type MySQLConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

// implements the DBConnector interface for MySQL.
type MySQLConnector struct {
	config MySQLConfig
	db     *sql.DB
}

// Kafka connection configuration.
type KafkaConfig struct {
	Broker string
}

// KafkaConnector implements the DBConnector interface for Kafka.
type KafkaConnector struct {
	config   KafkaConfig
	producer sarama.SyncProducer
	consumer sarama.Consumer
}

// ClickHouse connection configuration.
type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

// struct that implements the DBConnector interface for ClickHouse.
type ClickHouseConnector struct {
	config ClickHouseConfig
}

// NewKafkaConnector creates a new KafkaConnector.
func NewKafkaConnector(config KafkaConfig) (*KafkaConnector, error) {
	kc := &KafkaConnector{
		config: config,
	}
	err := kc.connect()
	if err != nil {
		return nil, err
	}
	return kc, nil
}

// Connect establishes a connection to Kafka.
func (kc *KafkaConnector) Connect() error {
	return kc.connect()
}

// Connect establishes a connection to Kafka.
func (kc *KafkaConnector) connect() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kc.config.Broker}, config)
	if err != nil {
		return err
	}

	kc.producer = producer
	return nil
}

// Close closes the Kafka producer.
func (kc *KafkaConnector) Close() error {
	if kc.producer != nil {
		return kc.producer.Close()
	}
	return errors.New("Kafka producer is not open")
}

// SendMessage sends a message to a Kafka topic.
func (kc *KafkaConnector) SendMessage(topic string, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := kc.producer.SendMessage(msg)
	return err
}

func DBMysql() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	dbUser := os.Getenv("MYSQL_USERNAME")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbHost := os.Getenv("MYSQL_HOSTNAME")
	dbPort := os.Getenv("MYSQL_PORT")
	dbName := os.Getenv("MYSQL_DBNAME")

	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {

		logs.Logger.Info("Error opening MySQL connection: %v", err)
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func DBClickhouse() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	// if err := godotenv.Load(); err != nil {
	// 	// loggy.Error(err.Error())
	// 	os.Exit(1)
	// }

	dbUser := os.Getenv("CLICKHOUSE_USERNAME")
	dbPassword := os.Getenv("CLICKHOUSE_PASSWORD")
	dbHost := os.Getenv("CLICKHOUSE_HOSTNAME")
	dbPort := os.Getenv("CLICKHOUSE_PORT")
	dbName := os.Getenv("CLICKHOUSE_DBNAME")

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s", dbHost, dbPort, dbUser, dbPassword, dbName)
	fmt.Println("Connecting to:", dataSourceName)

	db, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {

		logs.Logger.Info("Error opening  clickhouse connection: %v", err)
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func DB() (*sql.DB, error) {
	var db *sql.DB
	var err error
	connStr := "tcp://localhost:9000?username=default&password=zamap123&database=vinam_data"

	db, err = sql.Open("clickhouse", connStr)
	if err != nil {

		logs.Logger.Info("Error opening clickhouse connection: %v", err)
	}

	return db, nil
}
