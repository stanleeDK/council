package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const sendGridEndpoint = "https://api.sendgrid.com/v3/mail/send"

// Email configuration. Hardcode the addresses here; the API key is read from the
// SENDGRID_API_KEY environment variable (same as the Python app) so the secret
// never lands in source/git. The email is skipped (logged, no crash) whenever
// the key or either address is empty.
var (
	failureEmailTo   = "" // e.g. "k.r.cooke@gmail.com"
	failureEmailFrom = "" // e.g. "info@stanlee.info" (must be a SendGrid-verified sender)
)

// SendFailureReport emails a summary of this run's actionable failures
// (ERROR/CRITICAL) via SendGrid's HTTP API. It is best-effort: any missing
// configuration or transport error is logged and never crashes the app. The
// complete, durable record remains in logs/error_logs/errors.log; this email is
// just a heads-up summary.
func SendFailureReport(ea *ErrorAggregator) error {
	apiKey := os.Getenv("SENDGRID_API_KEY")
	to := failureEmailTo
	from := failureEmailFrom
	if apiKey == "" || to == "" || from == "" {
		log.Println("Failure email skipped: set SENDGRID_API_KEY env var and failureEmailTo/failureEmailFrom in email.go to enable it.")
		return nil
	}

	// Collect only actionable failures from the aggregator's in-memory records.
	var failures []ErrorRecord
	for _, rec := range ea.GetErrors() {
		if rec.Severity == SeverityError || rec.Severity == SeverityCritical {
			failures = append(failures, rec)
		}
	}
	if len(failures) == 0 {
		log.Println("Failure email skipped: no failures this run.")
		return nil
	}

	subject := fmt.Sprintf("Caption scraper: %d failure(s) on %s", len(failures), time.Now().Format("2006-01-02"))
	body := buildFailureEmailBody(failures)

	payload := map[string]interface{}{
		"personalizations": []map[string]interface{}{
			{"to": []map[string]string{{"email": to}}},
		},
		"from":    map[string]string{"email": from},
		"subject": subject,
		"content": []map[string]string{
			{"type": "text/plain", "value": body},
		},
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failure email skipped: could not marshal payload: %v", err)
		return err
	}

	req, err := http.NewRequest("POST", sendGridEndpoint, bytes.NewReader(jsonBody))
	if err != nil {
		log.Printf("Failure email skipped: could not build request: %v", err)
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failure email failed to send: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("Failure email: SendGrid returned %d: %s", resp.StatusCode, string(respBody))
		return fmt.Errorf("sendgrid status %d", resp.StatusCode)
	}

	log.Printf("Failure email sent to %s (%d failures).", to, len(failures))
	return nil
}

// buildFailureEmailBody renders a plain-text summary. Each line carries the URL
// (the re-run key for caption failures) and the message (which includes the
// yt-dlp command for channel failures).
func buildFailureEmailBody(failures []ErrorRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d failure(s) during this run.\n\n", len(failures))
	for _, rec := range failures {
		errMsg := ""
		if rec.Error != nil {
			errMsg = rec.Error.Error()
		}
		fmt.Fprintf(&b, "[%s] %s | Channel: %s | Component: %s\n",
			rec.Severity, rec.Timestamp.Format(time.RFC3339), rec.Channel, rec.Component)
		if rec.URL != "" {
			fmt.Fprintf(&b, "  URL: %s\n", rec.URL)
		}
		fmt.Fprintf(&b, "  %s: %s\n\n", rec.Message, errMsg)
	}
	b.WriteString("Full durable record: logs/error_logs/errors.log\n")
	return b.String()
}
