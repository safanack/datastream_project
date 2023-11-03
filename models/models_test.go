package models

import "testing"

func TestToCSV(t *testing.T) {
	testContact := Contacts{
		ID:      1,
		Name:    "John Doe",
		Email:   "johndoe@example.com",
		Details: "Some details",
	}

	expectedCSV := "1,John Doe,johndoe@example.com,Some details"
	if result := testContact.ToCSV(); result != expectedCSV {
		t.Errorf("ToCSV failed, expected %s, but got %s", expectedCSV, result)
	}
}
