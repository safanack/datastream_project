package config

import (
	"testing"
)

func TestConfig(t *testing.T) {

	mockConfig := KafkaConfig{
		Broker: "localhost:9092",
	}

	// Call the NewKafkaConnector function with the mock values
	connector, err := NewKafkaConnector(mockConfig)

	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	// For example, check if the connector is not nil
	if connector == nil {
		t.Errorf("Expected a non-nil connector, but got nil")
	}
}
