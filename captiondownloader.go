package main

import (
	"sync"
    "net/http"
    "fmt"
    "log"
    "os"
    "io"
    "time"
    "context"
)

type CaptionDownloadManager struct {
	ctx                                 context.Context
	CaptionsToBeDownloaded   			chan *VideoToBeDownloadedResult
	WaitG           					*sync.WaitGroup
	NumberOfCaptionSRTDownloadWorkers 	int
	errorAggregator						*ErrorAggregator
}

func NewCaptionDownloadManager(ctx context.Context, errorAggregator *ErrorAggregator, numworkers int) *CaptionDownloadManager {
	return &CaptionDownloadManager{
		ctx:                                ctx,
		WaitG: 								&sync.WaitGroup{},
		CaptionsToBeDownloaded:				make(chan *VideoToBeDownloadedResult, 100),
		NumberOfCaptionSRTDownloadWorkers: 	numworkers,
		errorAggregator:					errorAggregator,
	}
}

func (cdm *CaptionDownloadManager) Start() {
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
            cdm.httpRequestGetVideoCaptionsAndSaveToFile(downloadJob)
            // Errors are already recorded in the function with specific context

        case <-cdm.ctx.Done():
            // Context cancelled - graceful shutdown
            log.Printf("Caption download worker %d cancelled: %v", i, cdm.ctx.Err())
            return
        }
    }
}

func (cdm *CaptionDownloadManager)httpRequestGetVideoCaptionsAndSaveToFile(video *VideoToBeDownloadedResult) error {

    // Create context-aware HTTP request so it can be cancelled
    req, err := http.NewRequestWithContext(cdm.ctx, "GET", video.Captionurl, nil)
    if err != nil {
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Failed to create HTTP request for caption")
        return fmt.Errorf("failed to create request: %w", err)
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Failed to make HTTP request for caption")
        return fmt.Errorf("failed to make request: %w", err)
    }
    
    defer resp.Body.Close()
    
    // Check status code
    if resp.StatusCode != http.StatusOK {
        err := fmt.Errorf("unexpected status code: %d", resp.StatusCode)
        cdm.errorAggregator.RecordError(SeverityError, video.WorkerID, video.Channel, video.Originalurl, "network", err, "Unexpected HTTP status code when fetching caption")
        return err
    }

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
    
    return nil
}