package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Url struct {
	ParentUrl      string    `json:"parent_url"`
	PdfUrl         string    `json:"pdf_url"`
	LastModified   string    `json:"last_modified"`
	Etag           string    `json:"etag"`
	ExtractionTime time.Time `json:"extraction_time"`
}

type Out struct {
	StartTime       string        `json:"start_time"`
	TotalScrapeTime time.Duration `json:"total_scrape_time"`
	BlockedTotal    int           `json:"blocked_total"`
}

func WriteJsonToFile(collected []Url, name string) error {
	filePath := fmt.Sprintf("./json_out/%s.json", name)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(collected); err != nil {
		return err
	}

	return nil
}
