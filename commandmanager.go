package main

import (
	// "runtime"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	// "strconv"
	"encoding/json"
	// "time"
	"bufio"
)

/*
this contains all the ytp instances that need to be executed in separate goroutines, the output file details
the master context and it's cancel function
*/
type CommandManager struct {
	commands        []ChannelToBeScraped
	outputFile      string   // Path to the single output file
	outputHandle    *os.File // File handle for writing
	ctx             context.Context
	cancel          context.CancelFunc
	resultChan      chan VideoToBeDownloadedResult // child go routines running yt-dlp will send results to this channel
	wg              *sync.WaitGroup
	video_captions  map[string]VideoToBeDownloadedResult //map to hold all historical / previous output and also to hold future output; but only retrieve subtitles for videos not in the historical storage
	videoMapMutex   sync.RWMutex                         // Protects concurrent access to video_captions map
	errorAggregator *ErrorAggregator                     // Centralized error handling
	// done 			chan struct{} //channel to orchestrate the signalling of the end of the dedupe process to the main
}

func NewCommandManager(outputFile string, commands []ChannelToBeScraped, errorAggregator *ErrorAggregator) (*CommandManager, error) {

	//top level context for shutting down entire app
	ctx, cancel := context.WithCancel(context.Background())

	// Open output file, the final paramater goversn permissioning of the file at the os level
	file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		cancel()                                                      //use the context cancel to stop the app
		return nil, fmt.Errorf("failed to open output file: %w", err) //fmt.errorf creates an error obj which you can write to a log file later outisde this function
	}

	return &CommandManager{
		wg:              &sync.WaitGroup{},
		commands:        commands,
		outputFile:      outputFile,
		outputHandle:    file,
		ctx:             ctx,
		cancel:          cancel,
		resultChan:      make(chan VideoToBeDownloadedResult, 100),
		video_captions:  make(map[string]VideoToBeDownloadedResult, 100), //initialize the video map roughly with 100x the number of video channesl in the video_source.csv file
		errorAggregator: errorAggregator,
	}, nil
}

/*
this is kind of the orhestrator of all the command and jobs to be done
Launch each yt-dlp command in their own go routines. They're all housed in the CommandManager
have a separate go routine to wait for them all to finish and when they're done close the channel
another job to be done is putting all of the json blobs into a hashmap so they can be deduped (from there another struct will download srt)
*/
func (cm *CommandManager) Start() {

	// Start all commands
	for i, videolisttobescraped := range cm.commands {
		cm.wg.Add(1)
		go cm.CommandWorker(cm.ctx, cm.resultChan, i, videolisttobescraped)
	}

	// wait for all the commandworders to finish, and then close the channel which sends a signal back to main()
	// go func(){
	//     log.Println("Worker waiting to close result channel STARTING")
	//     defer log.Println("Worker waiting to close result channel ENDED")
	//     close(cm.resultChan)
	//     fmt.Println("closing result chan?")

	// }()

	// go func(){//read from the results channel in this goroutine, as soon as results come in; then place them into a hashmap. ..use empty struct to
	//     log.Println("Worker DeDupe result channel STARTING")
	//     defer log.Println("Worker DeDupe result channel ENDED")
	//     cm.DeDupeResultChanVideos()
	//     // cm.done <- struct{}{} // signal of the end of the dedupe process to main
	// }()

	log.Printf("Started - %d channels to be scraped", len(cm.commands))
}

/*
 */
func (cm *CommandManager) CommandWorker(ctx context.Context, results chan<- VideoToBeDownloadedResult, workerID int, scrape_vid_config ChannelToBeScraped) {
	// log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " PARENT Worker starting:",workerID,"num of goroutines: ", runtime.NumGoroutine())
	// defer log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " PARENT Worker ENDED")
	defer cm.wg.Done()

	cmd := exec.CommandContext(ctx, scrape_vid_config.Command, scrape_vid_config.Args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cm.errorAggregator.RecordError(SeverityError, workerID, scrape_vid_config.Channel,
			scrape_vid_config.Url, "yt-dlp", err, "Failed to create stdout pipe")
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cm.errorAggregator.RecordError(SeverityError, workerID, scrape_vid_config.Channel,
			scrape_vid_config.Url, "yt-dlp", err, "Failed to create stderr pipe")
		return
	}
	stdoutLines := make(chan VideoToBeDownloadedResult, 10) //channel to hold the output from the cmd yt-dlp (which is to stdout)

	// Start the command
	if err := cmd.Start(); err != nil {
		cm.errorAggregator.RecordError(SeverityCritical, workerID, scrape_vid_config.Channel,
			scrape_vid_config.Url, "yt-dlp", err, "Failed to start yt-dlp command")
		return
	}

	// Goroutine 1: Read stdout and put it into the stdOutLines Channel (stdout is a blocking I/O)
	go func() {
		// log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " STDOUT Worker starting:",workerID,"num of goroutines: ", runtime.NumGoroutine())
		defer close(stdoutLines)
		// defer log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " STDOUT Worker ENDED")

		decoder := json.NewDecoder(stdout)

		for {
			var obj map[string]interface{} // Use a map to hold the JSON data

			if err := decoder.Decode(&obj); err == io.EOF {
				break // End of input
			} else if err != nil {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", err, "Error decoding JSON from yt-dlp output")
				continue // Skip this entry and continue processing
			}

			// Safely extract fields with error handling
			upload_date, ok := obj["upload_date"].(string)
			if !ok {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", nil, "Missing upload_date in JSON")
				continue
			}
			id, ok := obj["id"].(string)
			if !ok {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", nil, "Missing id in JSON")
				continue
			}
			Channel, ok := obj["channel"].(string)
			if !ok {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", nil, "Missing channel in JSON")
				continue
			}
			captions, _ := obj["automatic_captions"].(map[string]interface{})
			Originalurl, ok := obj["original_url"].(string)
			if !ok {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", nil, "Missing original_url in JSON")
				continue
			}
			var captionurl string

			// extract captions if they exist
			if len(captions) > 0 {
				/* jsondump sample
				   "en": [
				    {
				      "ext": "json3",
				      "url": "https://www.youtube.com/api/timedtext?v=Vo8OBoIpXUU&ei=7W1TaMiLBez4sfIP_72PkQ0&caps=asr&opi=112496729&xoaf=5&hl=en&ip=0.0.0.0&ipbits=0&expire=1750323293&sparams=ip%2Cipbits%2Cexpire%2Cv%2Cei%2Ccaps%2Copi%2Cxoaf&signature=081AC6EFAA241FF2C2617DA6E9D06C4100DC93EA.C2DE5D5A03D33B2030DFCFF8897BAF9974D88BCB&key=yt8&kind=asr&lang=en&fmt=json3",
				      "name": "English",
				      "__yt_dlp_client": "tv"
				    },
				*/
				if englishcaptions, ok := captions["en"].([]interface{}); ok {
					for _, engCaptionURL := range englishcaptions {
						if engC, ok := engCaptionURL.(map[string]interface{}); ok {
							if ext, ok := engC["ext"].(string); ok && ext == "srt" {
								if url, ok := engC["url"].(string); ok {
									captionurl = url
								}
							}
						}
					}
				}
			}

			output := VideoToBeDownloadedResult{
				WorkerID:    workerID,
				Upload_date: upload_date,
				Id:          id,
				Captionurl:  captionurl,
				Channel:     Channel,
				Originalurl: Originalurl,
			}

			select {
			case stdoutLines <- output:				
				// Successfully queued stdout line
			case <-ctx.Done():
				// Context cancelled, stop reading
				return
			}
		}
	}()

	// Goroutine 2: Process all output from stderr which is a blocking I/O event that's why you need a go rotine
	go func() {
		// log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " STDERR Worker starting:",workerID,"num of goroutines: ", runtime.NumGoroutine())
		// defer log.Println(/*time.Now().Format("15:04:05.000"),*/scrape_vid_config.Url, " STDERR Worker ENDED")

		scanner := bufio.NewScanner(stderr)

		//record all errors from stderr (from yt-dlp) to file for diagnosis in event of
		for scanner.Scan() {
			cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
				scrape_vid_config.Url, "yt-dlp", fmt.Errorf(scanner.Text()), "yt-dlp stderr output")
		}

		// Check for scanner errors
		if err := scanner.Err(); err != nil {
			cm.errorAggregator.RecordError(SeverityError, workerID, scrape_vid_config.Channel,
				scrape_vid_config.Url, "yt-dlp", err, "Error reading from stderr scanner")
		}
	}()

	// Main worker event loop - coordinate and process streams
	for {
		select {
		case ytldlpresult, ok := <-stdoutLines: // ok = true then channel is still open / false if channel is closed
			if ok == false {//channel is closed because ok = false so get out of here
				goto cleanup
			}

			// Avoid videos you already have. Check existence of video in the hashmap 
			cm.videoMapMutex.RLock()
			_, exists := cm.video_captions[ytldlpresult.Id]
			cm.videoMapMutex.RUnlock()

			if exists {
				// video already has subtitles extracted or queue to be extracted; ignore it
				// OPTIMIZATION - COULD WE HAVE A ROLLING WINDOW TO EXCLUDE THIS VIDEO FROM CHANNEL JSON DUMPS? MAYBE HAVE A MOVING TERMINATION DATE
			} else {
				
				if ytldlpresult.Captionurl != "" {
					
					cm.videoMapMutex.Lock() // map cannot be accessed with goroutines without a lock
						log.Println("VIDEO FOUND ON:", ytldlpresult.Channel, "VIDEO ID:", ytldlpresult.Id, "NUMBER OF VIDEOS IN HASHMAP:", len(cm.video_captions))
						cm.video_captions[ytldlpresult.Id] = ytldlpresult
					cm.videoMapMutex.Unlock()

				} else {
					log.Println("Video has no captions:", ytldlpresult.Id,ytldlpresult.Channel)	
				}

			}
			// log.Println(cm.video_captions, len(cm.video_captions))
			results <- ytldlpresult
		case <-ctx.Done():
			// Context cancelled - kill command and cleanup
			cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel, scrape_vid_config.Url, "yt-dlp", ctx.Err(), "Context cancelled, terminating worker")
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			goto cleanup
		}
	}

cleanup:
	// Wait for command to finish and send completion result
	if err := cmd.Wait(); err != nil {
		// Actual error - yt-dlp failed
		cm.errorAggregator.RecordError(SeverityError, workerID, scrape_vid_config.Channel, scrape_vid_config.Url, "yt-dlp", err, "yt-dlp command failed")
	} else {
		// log.Printf("Worker %d finished successfully for channel %s", workerID, scrape_vid_config.Channel)
	}
}

// a blocking function that allows for orchestrating the completion of the concurrent population and deduping of the hashtable
func (cm *CommandManager) WaitForAllWorkToFinish() {
	cm.wg.Wait()
	close(cm.resultChan)
}

func (cm *CommandManager) makeResultsInHashMapAvailableToParameterChannel(srtVideoFilesToDownload chan<- *VideoToBeDownloadedResult) {
	// Lock while reading from the map
	cm.videoMapMutex.RLock()
	defer cm.videoMapMutex.RUnlock()

	for _, videoToBeDownloaded := range cm.video_captions {
		// Only send videos that actually have caption URLs
		if videoToBeDownloaded.Captionurl != "" {
			vid := videoToBeDownloaded
			srtVideoFilesToDownload <- &vid
		}
	}
	close(srtVideoFilesToDownload)

}
