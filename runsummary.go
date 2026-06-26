package main

import (
	"sync"
	"time"
)

// ScrapedVideo is a lightweight record of a video discovered/processed during a
// run, used only for the post-run summary email.
type ScrapedVideo struct {
	Channel    string
	Id         string
	UploadDate time.Time // when the video was posted to YouTube
	Url        string
}

// RunSummary accumulates what happened during a single run so it can be emailed
// at the end. It is safe for concurrent use by the scrape and download workers.
type RunSummary struct {
	mu                 sync.Mutex
	channels           map[string]struct{}
	videosScraped      []ScrapedVideo
	captionsDownloaded []ScrapedVideo
	startedAt          time.Time
}

func NewRunSummary() *RunSummary {
	return &RunSummary{
		channels:  make(map[string]struct{}),
		startedAt: time.Now(),
	}
}

// RecordChannelScraped notes that a channel was scraped (yt-dlp was run on it).
func (rs *RunSummary) RecordChannelScraped(channel string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.channels[channel] = struct{}{}
}

// RecordVideoScraped notes a newly discovered video (one with a caption URL,
// queued for download — already-downloaded videos are skipped upstream).
func (rs *RunSummary) RecordVideoScraped(v ScrapedVideo) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.videosScraped = append(rs.videosScraped, v)
}

// RecordCaptionDownloaded notes a caption file that was successfully written.
func (rs *RunSummary) RecordCaptionDownloaded(v ScrapedVideo) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.captionsDownloaded = append(rs.captionsDownloaded, v)
}

// Snapshot returns copies of the collected data for read-only use (e.g. the
// email builder), without holding the lock during formatting.
func (rs *RunSummary) Snapshot() (channels []string, videos, captions []ScrapedVideo, startedAt time.Time) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for c := range rs.channels {
		channels = append(channels, c)
	}
	videos = append(videos, rs.videosScraped...)
	captions = append(captions, rs.captionsDownloaded...)
	return channels, videos, captions, rs.startedAt
}
