package main

import (
	// "runtimgoe"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	// "strconv"
	"encoding/csv"
	"encoding/json"
	"time"
	"bufio"
)

/*
this contains all the ytp instances that need to be executed in separate goroutines, the output file details
the shared context passed from main
*/
type CommandManager struct {
	commands        chan ChannelToBeScraped
	inputFile   	string //path to file of channels to download
	ctx             context.Context
	resultChan      chan VideoToBeDownloadedResult // child go routines running yt-dlp will send results to this channel
	wg              *sync.WaitGroup
	numberofWorkers int
	video_captions  map[string]VideoToBeDownloadedResult //map to hold all historical / previous output and also to hold future output; but only retrieve subtitles for videos not in the historical storage
	videoMapMutex   sync.RWMutex // Protects concurrent access to video_captions map
	csvUpdateMutex  sync.Mutex   // Protects CSV file updates
	errorAggregator *ErrorAggregator // Centralized error handling
	// done 			chan struct{} //channel to orchestrate the signalling of the end of the dedupe process to the main
}

func NewCommandManager(ctx context.Context, inputFile string, errorAggregator *ErrorAggregator, numworkers int) (*CommandManager, error) {

	return &CommandManager{
		wg:              &sync.WaitGroup{},
		commands:        make(chan ChannelToBeScraped,100),
		numberofWorkers: numworkers,
		inputFile:		 inputFile,
		ctx:             ctx,
		resultChan:      make(chan VideoToBeDownloadedResult, 100),
		video_captions:  make(map[string]VideoToBeDownloadedResult, 100), //initialize the video map roughly with 100x the number of video channesl in the video_source.csv file
		errorAggregator: errorAggregator,
	}, nil
}

/*
this is kind of the orhestrator of all the command and jobs to be done
Have a finite set of workers work through all the jobs in the commands channel 

*/
func (cm *CommandManager) Start() {

	go cm.readCSVToStructs()

	for i:=0; i<cm.numberofWorkers;i++ {
		cm.wg.Add(1)
		go cm.StartCommandWorkers(i)
	}
}


func (cm *CommandManager) StartCommandWorkers(workerID int){

	defer cm.wg.Done()

	for youtubechannel := range cm.commands {
		cm.CommandWorker(workerID, youtubechannel)
	}

	// go cm.CommandWorker(cm.ctx, cm.resultChan, i, videolisttobescraped)
}

/*
 */
func (cm *CommandManager) CommandWorker(/*ctx context.Context, results chan<- VideoToBeDownloadedResult,*/ workerID int, scrape_vid_config ChannelToBeScraped) {
	
	// log.Println("Command Worker starting:",workerID,"num of goroutines: ", runtime.NumGoroutine(), scrape_vid_config)
	// defer log.Println("Command Worker ending:",workerID,"num of goroutines: ", runtime.NumGoroutine(),scrape_vid_config)

	// Track the youngest (most recent) upload date for this channel
	youngestUploadDate := scrape_vid_config.YoungestVideoUploaded_at

	cmd := exec.CommandContext(cm.ctx, scrape_vid_config.Command, scrape_vid_config.Args...)
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
	log.Printf("[Worker %d] Started command: %s", workerID, cmd.String())

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
			upload_date_str, ok := obj["upload_date"].(string)
			if !ok {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", nil, "Missing upload_date in JSON")
				continue
			}

			upload_date, err := time.Parse("20060102", upload_date_str)
			if err != nil {
				cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
					scrape_vid_config.Url, "yt-dlp", err, fmt.Sprintf("Invalid upload_date format: %s", upload_date_str))
				continue
			}

			// Update youngest upload date if this video is newer
			if upload_date.After(youngestUploadDate) {
				youngestUploadDate = upload_date
				log.Printf("[Worker %d] New youngest date for channel %s: %s", workerID, scrape_vid_config.Channel, youngestUploadDate.Format("2006-01-02"))
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
			case <-cm.ctx.Done():
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

		// Record all errors from stderr (from yt-dlp) to centralized error aggregator
		for scanner.Scan() {
			cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel,
				scrape_vid_config.Url, "yt-dlp", fmt.Errorf("%s", scanner.Text()), "yt-dlp stderr output")
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
			cm.resultChan <- ytldlpresult
		case <-cm.ctx.Done():
			// Context cancelled - kill command and cleanup
			cm.errorAggregator.RecordError(SeverityWarning, workerID, scrape_vid_config.Channel, scrape_vid_config.Url, "yt-dlp", cm.ctx.Err(), "Context cancelled, terminating worker")
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

	// Persist the youngest upload date back to CSV
	cm.persistYoungestUploadDate(scrape_vid_config.Channel, youngestUploadDate)
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


func (cm *CommandManager) readCSVToStructs()  {
    file, err := os.Open(cm.inputFile)
    if err != nil {
		cm.errorAggregator.RecordError(SeverityError, 0, "",
		"", "reading csv", err, "Failed to read videosources.csv")
		return
    }
    defer file.Close()

    reader 			:= csv.NewReader(file)
    reader.Comment   = '#' // Skip lines starting with #
    records, err 	:= reader.ReadAll()    // Read all records

    // Convert each row to a struct
    for i := 1; i < len(records); i++ {
        row := records[i]

        // Parse the youngest video date from CSV
        youngestDate, err := time.Parse("20060102", row[3])
        if err != nil {
            cm.errorAggregator.RecordError(SeverityWarning, 0, row[1], row[2],
                "csv-parse", err, fmt.Sprintf("Invalid date format in CSV: %s, using zero time", row[3]))
            youngestDate = time.Time{} // Use zero time as fallback
        }

        // Build the date value for --dateafter parameter
        var dateValue string

        // row[4] = IsRecentlyAdded; if it's a recently added channel, then you want to scrape up to a year's worth of transcripts
        if row[4] == "true" {
        	// build date parameter where the date is a year ago from today --dateafter 20250820
        	today 		:= time.Now().Truncate(24 * time.Hour)
        	oneYearAgo 	:= today.AddDate(-1, 0, 0)
        	dateValue = oneYearAgo.Format("20060102")
        } else  { //means IsRecentlyAdded = false and YoungestVideoUploaded_at is populated. Only scrape videos younger than the latest one
        	dateValue = youngestDate.Format("20060102")
        }

        record := ChannelToBeScraped{
            Platform:   				row[0],
            Channel:    				row[1],
            Url:        				row[2],
            Command:    				ytdlp_version,
            Args:       				[]string{ytdlpDumpJSON, ytdlpNoWarnings, ytdlpDateAfter, dateValue, ytdlp_cookiesparam, ytdlpCookiesFile, row[2]},
            YoungestVideoUploaded_at: 	youngestDate,
            IsRecentlyAdded: 			row[4],
        }

        cm.commands <- record
    }


    close(cm.commands)

}

// persistYoungestUploadDate updates the CSV file with the newest upload date for a channel
func (cm *CommandManager) persistYoungestUploadDate(channelName string, uploadDate time.Time) {
	cm.csvUpdateMutex.Lock()
	defer cm.csvUpdateMutex.Unlock()

	// Read the entire CSV file
	file, err := os.Open(cm.inputFile)
	if err != nil {
		log.Printf("Error opening CSV file for update: %v", err)
		return
	}
	defer file.Close()

	reader 		 := csv.NewReader(file)
	// Note: Don't set reader.Comment here - we need to preserve commented lines when rewriting
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading CSV file: %v", err)
		return
	}

	// Find and update the matching channel row (skip commented rows)
	updated := false
	for i := 1; i < len(records); i++ {
		row := records[i]
		// Skip commented rows
		if len(row) > 0 && strings.HasPrefix(row[0], "#") {
			continue
		}
		if len(row) > 3 && row[1] == channelName {
			row[4]		= "false" // since videos have been scraped, it's no longer new so you don't want to go back one year and to do that make this false
			row[3] 		= uploadDate.Format("20060102") // Update column 3 (YOUNGEST_VIDEO_UPLOADED_AT) - convert time.Time to string
			records[i] 	= row
			updated 	= true
			log.Printf("Updated CSV: Channel %s, new date: %s", channelName, uploadDate.Format("2006-01-02"))
			break
		}
	}

	if !updated {
		log.Printf("Warning: Channel %s not found in CSV, skipping update", channelName)
		return
	}

	// Write the updated CSV back to file
	file.Close() // Close read handle before writing

	writeFile, err := os.Create(cm.inputFile)
	if err != nil {
		log.Printf("Error creating CSV file for writing: %v", err)
		return
	}
	defer writeFile.Close()

	writer := csv.NewWriter(writeFile)
	err = writer.WriteAll(records)
	if err != nil {
		log.Printf("Error writing CSV file: %v", err)
		return
	}
	writer.Flush()

	if err := writer.Error(); err != nil {
		log.Printf("Error flushing CSV writer: %v", err)
	}
}

// this is a testing function to reset the state of video_sources.csv
// in the event of scraping failures like rate limiting you can go back 
// to an initial state (because once scraped the youngest_video_uploaded at date changs)
func (cm *CommandManager) testfunctionToResetCSVDates() {
	file, err := os.Open(cm.inputFile)
	if err != nil {
		log.Printf("Error opening CSV file for reset: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading CSV file: %v", err)
		return
	}

	for i := 1; i < len(records); i++ {
		row := records[i]
		if len(row) > 0 && strings.HasPrefix(row[0], "#") {
			continue
		}
		if len(row) > 4 {
			row[3] = "20260101"
			row[4] = "false"
			records[i] = row
		}
	}

	file.Close()

	writeFile, err := os.Create(cm.inputFile)
	if err != nil {
		log.Printf("Error creating CSV file for writing: %v", err)
		return
	}
	defer writeFile.Close()

	writer := csv.NewWriter(writeFile)
	err = writer.WriteAll(records)
	if err != nil {
		log.Printf("Error writing CSV file: %v", err)
		return
	}
	writer.Flush()

	if err := writer.Error(); err != nil {
		log.Printf("Error flushing CSV writer: %v", err)
	}

	log.Println("CSV dates reset: all channels set to 20260101, IS_RECENTLY_ADDED set to false")
}
