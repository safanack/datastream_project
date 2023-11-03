package models

import (
	"fmt"
	"strings"
	"time"
)

type Contacts struct {
	ID      int
	Name    string
	Email   string
	Details string
}

type ContactActivity struct {
	ContactID    int
	CampaignID   int
	ActivityType int
	ActivityDate time.Time
}

type ContactStatus struct {
	status int
}

func (c Contacts) ToCSV() string {
	// Convert Contacts fields to a CSV string
	fields := []string{
		fmt.Sprintf("%d", c.ID),
		c.Name,
		c.Email,
		c.Details,
	}
	return strings.Join(fields, ",")
}

func (ca ContactActivity) ToCSV() string {
	// Convert ContactActivity fields to a CSV string
	fields := []string{
		fmt.Sprintf("%d", ca.ContactID),
		fmt.Sprintf("%d", ca.CampaignID),
		fmt.Sprintf("%d", ca.ActivityType),
		fmt.Sprintf("%s", ca.ActivityDate.Format("2006-01-02 15:04:05")),
	}
	// fmt.Println(fields)
	return strings.Join(fields, ",")
}

type Data struct {
	Date    string `json:"date"`
	City    string `json:"city"`
	Country string `json:"country"`
}
type ResultRow struct {
	CampaignId int

	Abusive int
}
