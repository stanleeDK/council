package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity string

const (
	SeverityInfo    ErrorSeverity = "INFO"
	SeverityWarning ErrorSeverity = "WARNING"
	SeverityError   ErrorSeverity = "ERROR"
	SeverityCritical ErrorSeverity = "CRITICAL"
)

// ErrorRecord represents a single error occurrence
type ErrorRecord struct {
	Timestamp   time.Time
	Severity    ErrorSeverity
	WorkerID    int
	Channel     string
	URL         string
	Error       error
	Message     string
	Component   string // e.g., "yt-dlp", "caption-downloader", "file-io"
}

// ErrorAggregator collects and manages errors across all goroutines
type ErrorAggregator struct {
	errors         []ErrorRecord
	mu             sync.RWMutex
	errorChan      chan ErrorRecord
	done           chan struct{}
	wg             sync.WaitGroup
	stderrFiles    map[string]*os.File // Map of channel -> stderr file handle
	stderrFilesMu  sync.Mutex          // Mutex for stderrFiles map
}

// NewErrorAggregator creates a new error aggregator
func NewErrorAggregator() *ErrorAggregator {
	ea := &ErrorAggregator{
		errors:      make([]ErrorRecord, 0),
		errorChan:   make(chan ErrorRecord, 100),
		done:        make(chan struct{}),
		stderrFiles: make(map[string]*os.File),
	}
	
	// Start the error processing goroutine
	ea.wg.Add(1)
	go ea.processErrors()
	
	return ea
}

// RecordError records an error (thread-safe)
func (ea *ErrorAggregator) RecordError(severity ErrorSeverity, workerID int, channel, url, component string, err error, message string) {
	record := ErrorRecord{
		Timestamp: time.Now(),
		Severity:  severity,
		WorkerID:  workerID,
		Channel:   channel,
		URL:       url,
		Error:     err,
		Message:   message,
		Component: component,
	}
	
	select {
	case ea.errorChan <- record:
		// Successfully queued
	default:
		// Channel full, log directly (shouldn't happen but be safe)
		log.Printf("ERROR: Error channel full, logging directly: %v", record)
	}
}

// processErrors processes errors from the channel
func (ea *ErrorAggregator) processErrors() {
	defer ea.wg.Done()
	
	for {
		select {
		case errRecord := <-ea.errorChan:
			// Add to collection
			ea.mu.Lock()
			ea.errors = append(ea.errors, errRecord)
			ea.mu.Unlock()
			
			// Log the error
			ea.logError(errRecord)
			
		case <-ea.done:
			// Process remaining errors
			for {
				select {
				case errRecord := <-ea.errorChan:
					ea.mu.Lock()
					ea.errors = append(ea.errors, errRecord)
					ea.mu.Unlock()
					ea.logError(errRecord)
				default:
					return
				}
			}
		}
	}
}

// logError logs an error record
func (ea *ErrorAggregator) logError(record ErrorRecord) {
	errMsg := ""
	if record.Error != nil {
		errMsg = record.Error.Error()
	}
	
	log.Printf("[%s] Worker:%d Channel:%s Component:%s - %s: %s", 
		record.Severity, record.WorkerID, record.Channel, record.Component, record.Message, errMsg)
}

// GetErrors returns all collected errors (thread-safe)
func (ea *ErrorAggregator) GetErrors() []ErrorRecord {
	ea.mu.RLock()
	defer ea.mu.RUnlock()
	
	// Return a copy
	result := make([]ErrorRecord, len(ea.errors))
	copy(result, ea.errors)
	return result
}

// GetErrorSummary returns a summary of errors by severity
func (ea *ErrorAggregator) GetErrorSummary() map[ErrorSeverity]int {
	ea.mu.RLock()
	defer ea.mu.RUnlock()
	
	summary := make(map[ErrorSeverity]int)
	for _, err := range ea.errors {
		summary[err.Severity]++
	}
	return summary
}


// GetStderrFile gets or creates a stderr log file for a channel (thread-safe)
// Returns the file handle and any error. Caller should not close the file.
func (ea *ErrorAggregator) GetStderrFile(channel string) (*os.File, error) {
	ea.stderrFilesMu.Lock()
	defer ea.stderrFilesMu.Unlock()
	
	// Check if file already exists for this channel
	if file, exists := ea.stderrFiles[channel]; exists {
		return file, nil
	}
	
	// Create new file
	filePath := "logs/error_logs/ERROR LOG FILE -" + channel + "-" + time.Now().Format(time.DateTime)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr log file for channel %s: %w", channel, err)
	}
	
	// Store in map
	ea.stderrFiles[channel] = file
	return file, nil
}

// WriteStderr writes a line to the stderr file for a channel (thread-safe)
func (ea *ErrorAggregator) WriteStderr(channel, line string) error {
	file, err := ea.GetStderrFile(channel)
	if err != nil {
		return err
	}
	
	_, err = file.WriteString(line + "\n")
	return err
}

// CloseStderrFile closes the stderr file for a channel (thread-safe)
func (ea *ErrorAggregator) CloseStderrFile(channel string) error {
	ea.stderrFilesMu.Lock()
	defer ea.stderrFilesMu.Unlock()
	
	file, exists := ea.stderrFiles[channel]
	if !exists {
		return nil // Already closed or never opened
	}
	
	err := file.Close()
	delete(ea.stderrFiles, channel)
	return err
}

// CloseAllStderrFiles closes all open stderr files (thread-safe)
func (ea *ErrorAggregator) CloseAllStderrFiles() {
	ea.stderrFilesMu.Lock()
	defer ea.stderrFilesMu.Unlock()
	
	for channel, file := range ea.stderrFiles {
		file.Close()
		delete(ea.stderrFiles, channel)
	}
}

// Shutdown gracefully shuts down the error aggregator
func (ea *ErrorAggregator) Shutdown() {
	close(ea.done)
	ea.wg.Wait()
	close(ea.errorChan)
	ea.CloseAllStderrFiles()
}

// PrintSummary prints a summary of all errors
func (ea *ErrorAggregator) PrintSummary() {
	summary := ea.GetErrorSummary()
	errors := ea.GetErrors()
	
	log.Println("=== ERROR SUMMARY ===")
	log.Printf("Total errors: %d", len(errors))
	for severity, count := range summary {
		log.Printf("  %s: %d", severity, count)
	}
	
	// Print critical errors
	if summary[SeverityCritical] > 0 {
		log.Println("\n=== CRITICAL ERRORS ===")
		for _, err := range errors {
			if err.Severity == SeverityCritical {
				log.Printf("  [%s] %s - %s: %v", 
					err.Timestamp.Format(time.RFC3339), err.Channel, err.Message, err.Error)
			}
		}
	}
}


