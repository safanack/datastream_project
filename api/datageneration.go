package api

import (
	"datastream/models"
	"fmt"

	"math/rand"

	"time"
)

var activityDate1, activityDate2, activityDate3 time.Time

var activityDateX, activityDate time.Time

var i int

var flag int

var ActivityString string
var ContactActivities []models.ContactActivity

func calculateActivityDate() {

	activityDate1 = activityDate.AddDate(0, 0, 1)

	activityDate2 = activityDate1.AddDate(0, 0, 2)

	activityDate3 = activityDate2.AddDate(0, 0, 3)

}

func calculateActivity(id int) {

	percent := rand.Intn(101)

	if percent <= 80 {

		if percent <= 30 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 60 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		} else {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
		}

	} else if percent <= 90 {

		if percent <= 82 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 84 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		} else if percent <= 86 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 88 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		} else if percent <= 89 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
			activityDate = activityDate2

			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3

			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		}

	} else {

		percent := rand.Intn(1001)

		if percent <= 940 {

			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
		} else {

			flag = 0

			if percent <= 960 {

				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1

				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2

				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3

				ActivityString += fmt.Sprintf("(%d, %d, 5, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 5, activityDate.Format("2006-01-02"))
			} else if percent <= 970 {

				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1

				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2

				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3

				ActivityString += fmt.Sprintf("(%d, %d, 6, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 6, activityDate.Format("2006-01-02"))
			} else if percent <= 980 {

				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1

				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2

				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3

				ActivityString += fmt.Sprintf("(%d, %d, 5, \"%s\"),", id, i, activityDate.Format("2006-01-02"))

				ActivityString += fmt.Sprintf("(%d, %d, 6, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 5, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 6, activityDate.Format("2006-01-02"))
			} else {

				ActivityString += fmt.Sprintf("(%d, %d, 2, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 2, activityDate.Format("2006-01-02"))
			}

		}

	}

}

func generateData(id int) {

	i++

	if i%20 == 0 {

		activityDateX = activityDateX.AddDate(0, 1, 0)

		activityDate = activityDateX

		calculateActivityDate()

	} else {

		activityDate = activityDateX

	}

	// percent := rand.Intn(101)

	calculateActivity(id)

	if i == 100 || flag == 0 {

		ActivityString = ActivityString[:len(ActivityString)-1]

	} else {

		generateData(id)

	}

}

func CallActivity(id int) []models.ContactActivity {

	activityDateX, _ = time.Parse("2006-01-02", "2023-07-01")

	activityDate = activityDateX

	calculateActivityDate()

	i = 0

	flag = 1

	ActivityString = ""

	generateData(id)

	return ContactActivities
}

func appendContactActivity(contactID, campaignID, activityType int, activityDateStr string) {
	activityDate, err := time.Parse("2006-01-02", activityDateStr)

	if err != nil {

		//

		fmt.Printf("Error parsing date: %s\n", err)

		return

	}
	// Create a ContactActivity instance and append it to the slice

	activity := models.ContactActivity{
		ContactID:    contactID,
		CampaignID:   campaignID,
		ActivityType: activityType,
		ActivityDate: activityDate,
	}

	ContactActivities = append(ContactActivities, activity)
	// fmt.Printf("%d, %d, %d, %s", ContactActivities.ContactID, ContactActivities.CampaignID, ContactActivities.ActivityType, ContactActivities.ActivityDate)
}
