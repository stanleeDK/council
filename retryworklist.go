package main

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const retryDateLayout = "20060102"

// RetryEntry is one outstanding failed caption download, kept so it can be
// re-fetched later by the retry runner (the -r flag — separate TODO item).
type RetryEntry struct {
	Channel    string
	VideoID    string
	VideoURL   string // recovery key: re-dump this to mint a fresh caption URL
	UploadDate time.Time
}

// RetryWorklist is the durable list of videos whose caption download failed
// permanently (e.g. a 429 surviving all retries). It is keyed by video URL so a
// repeated failure never adds a duplicate row, and a successful download removes
// its entry — so the file only ever holds outstanding failures.
//
// Entries live in memory during a run and are written once at the end (Flush),
// atomically. Losing an unflushed change is self-correcting: an unrecorded
// failure simply re-fails (and is recorded) next run, and an unpruned success is
// skipped next time by the output_captions dedup. We store the VIDEO URL, not
// the caption URL, because caption URLs carry an `expire=` param and are dead
// later.
type RetryWorklist struct {
	mu      sync.Mutex
	entries map[string]RetryEntry // keyed by video URL
	path    string
}

// NewRetryWorklist loads any existing worklist at path into memory.
func NewRetryWorklist(path string) *RetryWorklist {
	rw := &RetryWorklist{
		entries: make(map[string]RetryEntry),
		path:    path,
	}
	rw.load()
	return rw
}

func (rw *RetryWorklist) load() {
	f, err := os.Open(rw.path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("retry worklist: could not open %s: %v", rw.path, err)
		}
		return // no list yet (first run)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("retry worklist: could not read %s: %v", rw.path, err)
		return
	}
	for i, row := range records {
		if i == 0 || len(row) < 4 { // skip header / malformed rows
			continue
		}
		date, _ := time.Parse(retryDateLayout, row[3])
		rw.entries[row[2]] = RetryEntry{
			Channel:    row[0],
			VideoID:    row[1],
			VideoURL:   row[2],
			UploadDate: date,
		}
	}
}

// RecordFailure adds a failed video to the worklist. Dedup is automatic: the same
// video URL maps to the same key, so a repeated failure overwrites rather than
// duplicating. Safe for concurrent use by the caption workers.
func (rw *RetryWorklist) RecordFailure(channel, videoID, videoURL string, uploadDate time.Time) {
	if videoURL == "" {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.entries[videoURL] = RetryEntry{Channel: channel, VideoID: videoID, VideoURL: videoURL, UploadDate: uploadDate}
}

// RemoveSuccess prunes a video from the worklist once its caption has downloaded.
func (rw *RetryWorklist) RemoveSuccess(videoURL string) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	delete(rw.entries, videoURL)
}

// Entries returns a snapshot of the current worklist (for the retry runner / tests).
func (rw *RetryWorklist) Entries() []RetryEntry {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	out := make([]RetryEntry, 0, len(rw.entries))
	for _, e := range rw.entries {
		out = append(out, e)
	}
	return out
}

// Flush writes the current worklist to disk atomically (temp file + rename). Call
// once at the end of a run.
func (rw *RetryWorklist) Flush() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	tmp, err := os.CreateTemp(filepath.Dir(rw.path), filepath.Base(rw.path)+".tmp-*")
	if err != nil {
		log.Printf("retry worklist: could not create temp file: %v", err)
		return
	}
	tmpName := tmp.Name()

	writer := csv.NewWriter(tmp)
	writer.Write([]string{"channel", "video_id", "video_url", "upload_date"})
	for _, e := range rw.entries {
		date := ""
		if !e.UploadDate.IsZero() {
			date = e.UploadDate.Format(retryDateLayout)
		}
		writer.Write([]string{e.Channel, e.VideoID, e.VideoURL, date})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Printf("retry worklist: error writing %s: %v", rw.path, err)
		tmp.Close()
		os.Remove(tmpName)
		return
	}
	if err := tmp.Close(); err != nil {
		log.Printf("retry worklist: error closing temp file: %v", err)
		os.Remove(tmpName)
		return
	}
	if err := os.Rename(tmpName, rw.path); err != nil {
		log.Printf("retry worklist: could not replace %s: %v", rw.path, err)
		os.Remove(tmpName)
		return
	}
	log.Printf("retry worklist: %d outstanding failed caption(s) recorded in %s", len(rw.entries), rw.path)
}
