package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const sendGridEndpoint = "https://api.sendgrid.com/v3/mail/send"

// Email configuration. Hardcode the addresses here; the API key is read from the
// SENDGRID_API_KEY environment variable (same as the Python app) so the secret
// never lands in source/git. An email is skipped (logged, no crash) whenever the
// key or either address is empty.
var (
	emailTo   = "stan.lee@outlook.dk"
	emailFrom = "info@stanlee.info" // must be a SendGrid-verified sender
)

// sendEmail POSTs a plain-text email through SendGrid's v3 Mail Send API. The
// API key comes from SENDGRID_API_KEY. Missing key/addresses are treated as a
// logged no-op (returns nil) so email never crashes the app.
func sendEmail(to, from, subject, plainBody string) error {
	apiKey := os.Getenv("SENDGRID_API_KEY")
	if apiKey == "" || to == "" || from == "" {
		log.Println("Email skipped: set SENDGRID_API_KEY env var and the to/from addresses to enable it.")
		return nil
	}

	payload := map[string]interface{}{
		"personalizations": []map[string]interface{}{
			{"to": []map[string]string{{"email": to}}},
		},
		"from":    map[string]string{"email": from},
		"subject": subject,
		"content": []map[string]string{
			{"type": "text/plain", "value": plainBody},
		},
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Email skipped: could not marshal payload: %v", err)
		return err
	}

	req, err := http.NewRequest("POST", sendGridEndpoint, bytes.NewReader(jsonBody))
	if err != nil {
		log.Printf("Email skipped: could not build request: %v", err)
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Email failed to send: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("Email: SendGrid returned %d: %s", resp.StatusCode, string(respBody))
		return fmt.Errorf("sendgrid status %d", resp.StatusCode)
	}

	log.Printf("Email sent to %s.", to)
	return nil
}

// SendFailureReport emails a summary of this run's actionable failures
// (ERROR/CRITICAL) via SendGrid's HTTP API. It is best-effort: any missing
// configuration or transport error is logged and never crashes the app. The
// complete, durable record remains in logs/error_logs/errors.log; this email is
// just a heads-up summary.
func SendFailureReport(ea *ErrorAggregator) error {
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
	return sendEmail(emailTo, emailFrom, subject, buildFailureEmailBody(failures))
}

// SendRunSummary emails a summary of what the run scraped and downloaded. Fired
// after every run when enabled (the -e flag).
func SendRunSummary(rs *RunSummary) error {
	channels, videos, captions, startedAt := rs.Snapshot()
	subject := fmt.Sprintf("Caption scraper run %s: %d channels, %d videos, %d captions",
		time.Now().Format("2006-01-02"), len(channels), len(videos), len(captions))
	return sendEmail(emailTo, emailFrom, subject,
		buildRunSummaryBody(channels, videos, captions, startedAt))
}

// buildRunSummaryBody renders the plain-text run summary, including the YouTube
// upload date for each video/caption.
func buildRunSummaryBody(channels []string, videos, captions []ScrapedVideo, startedAt time.Time) string {
	const dateFmt = "2006-01-02"
	uploadDate := func(t time.Time) string {
		if t.IsZero() {
			return "unknown"
		}
		return t.Format(dateFmt)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Run started: %s\n", startedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Run finished: %s\n\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&b, "Channels scraped: %d\n", len(channels))
	fmt.Fprintf(&b, "Videos scraped: %d\n", len(videos))
	fmt.Fprintf(&b, "Captions downloaded: %d\n\n", len(captions))

	sort.Strings(channels)
	b.WriteString("== Channels scraped ==\n")
	for _, c := range channels {
		fmt.Fprintf(&b, "  - %s\n", c)
	}

	b.WriteString("\n== Videos scraped (posted date) ==\n")
	for _, v := range videos {
		fmt.Fprintf(&b, "  [%s] %s | %s\n", uploadDate(v.UploadDate), v.Channel, v.Id)
	}

	b.WriteString("\n== Captions downloaded (posted date) ==\n")
	for _, v := range captions {
		fmt.Fprintf(&b, "  [%s] %s | %s\n", uploadDate(v.UploadDate), v.Channel, v.Id)
	}

	return b.String()
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
