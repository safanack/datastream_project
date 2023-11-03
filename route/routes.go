package route

import (
	"datastream/api"
	"net/http"
)

func SetupRoutes() {
	http.HandleFunc("/", api.UploadPageHandler)
	http.HandleFunc("/upload", api.UploadToKafka)
	http.HandleFunc("/GetResult", api.GetDataFromClickHouse)
}
