package main

import (
	// "fmt"
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
	errorLogFile   *os.File     // dedicated file for ERROR/CRITICAL records (logs/error_logs/errors.log)
	errorLogger    *log.Logger  // writes the failures-only log; nil if the file couldn't be opened
}

// NewErrorAggregator creates a new error aggregator
func NewErrorAggregator() *ErrorAggregator {
	ea := &ErrorAggregator{
		errors:      make([]ErrorRecord, 0),
		errorChan:   make(chan ErrorRecord, 100),
		done:        make(chan struct{}),
	}

	// Open a dedicated, greppable file containing only the actionable failures
	// (ERROR/CRITICAL). Best-effort: if it can't be opened we fall back to the
	// main logger only, so a missing directory/permission never crashes the app.
	if f, err := os.OpenFile("logs/error_logs/errors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
		log.Printf("WARNING: Failed to open error log file logs/error_logs/errors.log: %v. Errors will still go to the main log.", err)
	} else {
		ea.errorLogFile = f
		ea.errorLogger = log.New(f, "", log.LstdFlags)
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

	// Format includes URL so the failed resource (e.g. the video URL for a
	// caption failure) is captured inline and can be grepped for re-runs.
	format := "[%s] Worker:%d Channel:%s Component:%s URL:%s - %s: %s"
	args := []interface{}{record.Severity, record.WorkerID, record.Channel, record.Component, record.URL, record.Message, errMsg}

	log.Printf(format, args...)

	// Mirror actionable failures into the dedicated, failures-only error log.
	if ea.errorLogger != nil && (record.Severity == SeverityError || record.Severity == SeverityCritical) {
		ea.errorLogger.Printf(format, args...)
	}
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


// Shutdown gracefully shuts down the error aggregator
func (ea *ErrorAggregator) Shutdown() {
	close(ea.done)
	ea.wg.Wait()
	close(ea.errorChan)
	if ea.errorLogFile != nil {
		ea.errorLogFile.Close()
	}
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


