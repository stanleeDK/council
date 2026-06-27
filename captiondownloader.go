package main

import (
	"sync"
    "net/http"
    "fmt"
    "log"
    "os"
    "io"
    "math/rand"
    "strconv"
    "time"
    "context"
)

const (
    // captionRequestsPerSecond caps the aggregate request rate to YouTube across
    // ALL caption workers. This is the single knob to tune: start conservative,
    // watch the 429 rate, and raise it until you find the ceiling. It is
    // intentionally decoupled from the worker count.
    captionRequestsPerSecond = 5.0

    // captionMaxRetries is the number of retries (in addition to the first try)
    // for a single caption. Kept small on purpose: the timedtext URL carries an
    // `expire` param, so an open-ended retry would eventually fail with an
    // expiry error anyway.
    captionMaxRetries = 3

    captionBaseBackoff = 1 * time.Second
    captionMaxBackoff  = 8 * time.Second

    captionHTTPTimeout = 30 * time.Second
)

type CaptionDownloadManager struct {
	ctx                                 context.Context
	CaptionsToBeDownloaded   			chan *VideoToBeDownloadedResult
	WaitG           					*sync.WaitGroup
	NumberOfCaptionSRTDownloadWorkers 	int
	errorAggregator						*ErrorAggregator
	runSummary							*RunSummary
	retryWorklist						*RetryWorklist
	limiter								*RateLimiter
	httpClient							*http.Client
}

func NewCaptionDownloadManager(ctx context.Context, errorAggregator *ErrorAggregator, runSummary *RunSummary, retryWorklist *RetryWorklist, numworkers int) *CaptionDownloadManager {
	return &CaptionDownloadManager{
		ctx:                                ctx,
		WaitG: 								&sync.WaitGroup{},
		CaptionsToBeDownloaded:				make(chan *VideoToBeDownloadedResult, 100),
		NumberOfCaptionSRTDownloadWorkers: 	numworkers,
		errorAggregator:					errorAggregator,
		runSummary:							runSummary,
		retryWorklist:						retryWorklist,
		limiter:							NewRateLimiter(captionRequestsPerSecond),
		httpClient:							&http.Client{Timeout: captionHTTPTimeout},
	}
}

func (cdm *CaptionDownloadManager) Start() {
    fmt.Println("Number of caption files to be downloaded: ", len(cdm.CaptionsToBeDownloaded))
    defer cdm.limiter.Stop()
	for i:=0; i<cdm.NumberOfCaptionSRTDownloadWorkers;i++{
		cdm.WaitG.Add(1)
		go cdm.WorkerGetVideoCaptions(i)
	}
    cdm.WaitG.Wait()
}

func (cdm *CaptionDownloadManager)WorkerGetVideoCaptions(i int)  {
    // log.Println("SRT Downloader started: ", i)
    // defer log.Println("SRT Downloader ended: ", i)
	defer cdm.WaitG.Done()

    for {
        select {
        case downloadJob, ok := <-cdm.CaptionsToBeDownloaded:
            if !ok {
                // Channel closed, worker can exit
                return
            }
            // Errors are already recorded (with context) inside the function. Here
            // we additionally manage the retry worklist: a permanent failure adds
            // the video (keyed by URL, deduped), a success prunes it. A context
            // cancellation is a shutdown, not a caption failure — skip it.
            err := cdm.httpRequestGetVideoCaptionsAndSaveToFile(downloadJob)
            if err != nil {
                if cdm.ctx.Err() == nil {
                    cdm.retryWorklist.RecordFailure(downloadJob.Channel, downloadJob.Id, downloadJob.Originalurl, downloadJob.Upload_date)
                }
            } else {
                cdm.retryWorklist.RemoveSuccess(downloadJob.Originalurl)
            }

        case <-cdm.ctx.Done():
            // Context cancelled - graceful shutdown
            log.Printf("Caption download worker %d cancelled: %v", i, cdm.ctx.Err())
            return
        }
    }
}

func (cdm *CaptionDownloadManager)httpRequestGetVideoCaptionsAndSaveToFile(video *VideoToBeDownloadedResult) error {

    // resp holds the successful (HTTP 200) response once the retry loop breaks.
    var resp *http.Response

    for attempt := 0; ; attempt++ {
        // Global rate limit: gate every network attempt (including retries) so
        // the aggregate request rate across all workers stays under YouTube's
        // throttling threshold. Returns on context cancellation.
        if err := cdm.limiter.Wait(cdm.ctx); err != nil {
            return err
        }

        // Create context-aware HTTP request so it can be cancelled
        req, err := http.NewRequestWithContext(cdm.ctx, "GET", video.Captionurl, nil)
        if err != nil {
            cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Failed to create HTTP request for caption")
            return fmt.Errorf("failed to create request: %w", err)
        }

        r, err := cdm.httpClient.Do(req)
        if err != nil {
            // If the context was cancelled we are shutting down; do not retry.
            if cdm.ctx.Err() != nil {
                return cdm.ctx.Err()
            }
            // Transient network error: retry until the budget is exhausted.
            if attempt < captionMaxRetries {
                cdm.backoff(attempt, "")
                continue
            }
            cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Failed to make HTTP request for caption (retries exhausted)")
            return fmt.Errorf("failed to make request: %w", err)
        }

        if r.StatusCode == http.StatusOK {
            resp = r
            break
        }

        // Non-200. Retry 429 (throttling) and 5xx (transient server errors);
        // give up immediately on everything else (e.g. 403/410 = expired URL).
        retryAfter := r.Header.Get("Retry-After")
        retryable := r.StatusCode == http.StatusTooManyRequests || (r.StatusCode >= 500 && r.StatusCode < 600)
        statusCode := r.StatusCode
        r.Body.Close()

        if retryable && attempt < captionMaxRetries {
            cdm.backoff(attempt, retryAfter)
            continue
        }

        err = fmt.Errorf("unexpected status code: %d", statusCode)
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Unexpected HTTP status code when fetching caption (retries exhausted)")
        return err
    }

    defer resp.Body.Close()

    // Use triple underscore delimiter and ISO-8601 style timestamp (filename-safe)
    timestamp := time.Now().Format("2006-01-02T15-04-05")
    captionOutputFile ,err := os.Create("output_captions/" + video.Channel + "___" + video.Id + "___" + timestamp) 
    if err != nil { 
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "file-io", err, "Failed to create caption output file")
        return err
    }
    
    defer captionOutputFile.Close()

    //this writes the srt meta data from the get request to the output file
    fmt.Fprintf(captionOutputFile,"%s,%s,%s,%s,%s",video.Channel,video.Id,video.Upload_date,video.Captionurl,video.Originalurl)

    //this writes the actual srt caption content to the output file directory for later consumption     
    _, err = io.Copy(captionOutputFile, resp.Body)
    if err != nil { 
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "file-io", err, "Failed to copy caption content to file")
        return fmt.Errorf("failed to read response: %w", err)
    }
    
    log.Printf("Caption File Written: %s %s\n",video.Channel, video.Originalurl)

    cdm.runSummary.RecordCaptionDownloaded(ScrapedVideo{
        Channel:    video.Channel,
        Id:         video.Id,
        UploadDate: video.Upload_date,
        Url:        video.Originalurl,
    })

    return nil
}

// backoff sleeps before the next retry attempt. If the server sent a Retry-After
// header (delay in seconds) we honor it; otherwise we use capped exponential
// backoff (1s, 2s, 4s, ...) with jitter to avoid thundering-herd retries. The
// wait is capped (captionMaxBackoff) because the timedtext URL expires, so a
// long sleep would just trade a 429 for an expiry failure. Aborts early if the
// context is cancelled.
func (cdm *CaptionDownloadManager) backoff(attempt int, retryAfter string) {
    var d time.Duration

    if retryAfter != "" {
        if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
            d = time.Duration(secs) * time.Second
            if d > captionMaxBackoff {
                d = captionMaxBackoff
            }
        }
    }

    if d == 0 {
        d = captionBaseBackoff * time.Duration(1<<attempt) // 1s, 2s, 4s, ...
        if d > captionMaxBackoff {
            d = captionMaxBackoff
        }
        // Full jitter over [d/2, d] to spread out concurrent retries.
        d = d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
    }

    select {
    case <-time.After(d):
    case <-cdm.ctx.Done():
    }
}