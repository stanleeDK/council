package main

import (
	"log"
	"os"
	"testing"
)

func TestResetTestEnvironment(t *testing.T) {
	// Reset CSV dates
	cm := &CommandManager{
		inputFile: "video_sources.csv",
	}
	cm.testfunctionToResetCSVDates()

	// Clear application log
	if err := os.Remove("logs/application_logs/app.log"); err != nil {
		log.Printf("Could not remove app.log: %v", err)
	} else {
		log.Println("Removed logs/application_logs/app.log")
	}

	// Clear error logs
	entries, err := os.ReadDir("logs/error_logs")
	if err != nil {
		log.Printf("Could not read error_logs directory: %v", err)
	} else {
		for _, entry := range entries {
			os.RemoveAll("logs/error_logs/" + entry.Name())
		}
		log.Printf("Cleared logs/error_logs/ (%d files removed)", len(entries))
	}

	// Clear output_captions
	entries, err = os.ReadDir("output_captions")
	if err != nil {
		log.Printf("Could not read output_captions directory: %v", err)
	} else {
		for _, entry := range entries {
			os.RemoveAll("output_captions/" + entry.Name())
		}
		log.Printf("Cleared output_captions/ (%d entries removed)", len(entries))
	}
}
