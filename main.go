/*
- SYNOPSIS 
- READ ALSO NOTES FILE AND NOTES ON CONCURRENCY FILE 
- You have three structs
- ChannelToBeScraped - information from the csv file on channels you intend to scrape using yt-dlp
- VideoToBeDownloadedResult - this is the output from the yt-dlp process instant, that with the json dump command you are getting all meta data. You're after the srt url (subtitle url)
- CommandManager - this is an absraction that helps hold all the yt-dlp instance you want to spin up, the output file, the waitgroup and the the results channel into which all go routine workers will insert output

1. You start off by reading the csv file video_sources.csv and create a command manager to hold instance of channeltobescraped
2. in Start() you fire off the parent goroutines for each video. Each video has it's own yt-dlp process via the cmd package 
3. All the parent worker goroutines will write to a results channel which is an attributie of the command manager. 
4. GoRoutine ORchestration. Now because multiple goroutines are writing to a single channel, you can't just have one goroutine close the channel. 
and you want to close the channel to prevent memory leaks. Your main for select statement in the main func goroutine will also hang forever because
it will constantly check the results channel. That's why you need to use waitgroups. You create the waitgroup at the level where you create the goroutines. 
Everytime you create a goroutine you call .Add(1). see the Start() func. Then in that same level (func) where teh goroutines were created you create another goroutine
to monitor for when all the routines are done, you do that using wait(). And in there you call the close channel funciton, which will close the main channel 
when all the child routines are done, which in turn in your main funcc will release exection from the main for/select event loop by 
sending the channel closed message/event/ 
5. In the workder threads, you use the cmd exec library to fire up the yt-dlp instances. You are calling it with --dump-json which retunrns a massive
json blob with a ton of meta data. The idea is to grab the subtitle urls from it, and download those 
[insert logic on hashmap and persistence of what you already have scraped]
6. yt-dlp being a command line app, offers two streams stdout and stderr. These are streams that provide feedback on the executiong. you need to 
handle these pipes of information. You want to handle stderr separately because you want to be able to record problems independnelty so you an 
focus on those during debugging in production. 
7. Both of these are blocking events, ie they block the worker goroutines. So to make that more efficient you creaet 2 child goroutines, one for stdout 
and another for stderr.
8. STDOUT - this is the one which will read the output from the yt-dlp process (which is a another command line app seaprate from yours). The output from here
will go into the results channel but first you need a bridge channel stdoutLines. You send teh output from the json to this channel and when done the yt-dlp itself will close the channel which 
causes the event loop inside this stdout goroutine to self terminate. The workrer level parent event loop reads from this child channel and puts it into the results channel 
9. STDERR - stderr output from yt-dlp is captured and logged through the centralized ErrorAggregator using RecordError().
This simplifies error tracking by consolidating all errors in one place.

10. MAIN WORKER EVENT LOOP Meanwhile back in the main worker goroutine you have an event loop (which manifest itself as a for select loop) which will read from the stdout bridge channel. As soon as yt-dlp is done
it will cause the bridge channel to close, So this goroutines reads from the bridge channel and puts that output into the results channel. Remember other worker peer routines are also putting 
output into the results channel at the same time. You then go to cmd.wait which waits for yt-dlp to terminate, one that doess the defer waitgroup close staetement runs which dcreements 
the counter from the STart()
BACK IN COMMANDMANAGER.START()

12 Once all the waitgroup counters are decremened, then the results channel is finished producing (consuming from 
all the child stedout bridgechannels)) and you close that channel 

IRRELEVANT 
13 by closing the results channel the main event loop in the main function can close. 
14 before the main func event loop closees, it's also  in it's own event loop checking for output from the results 
channel and doing stuff to iit (you will write this to an ouput file and eventually)
IRRELEVANT

13a instead of having the result channel be read using a select loop in the main function, you instead
read the results right from stdoutlines in the worker thread to the map. Then the way to delay the main thread
(block the main thread/routine) is to have a specific wait function in the command manager. The CM has 
the instance in main, and if the main goroutine alls the waitgroup's wait function then main is blocked until
all the defer.wait.done are called from all the child routines. This approach avoids having another sync wait group
just for the 5 workers who are gong to download the srt files from the url in the jsonblob that came from the workers. 
The downside is the resultchan you had to house all the results from all the commands is kinda useless but lets keep it around 


SRT CAPTION DOWNLOAD  
1. idea here is to have a finite set of workers (5) to read from a hash containing all the json blobs you downloaded
This is achieved using the video_captions hashmap which is populated by DeDupeResultChanVideos(). The way it does this 
concurrently is that it is laundhed in the commandmanager's start() as a goroutine and it will have a for-select loop
to listen in on the results channel. As soon as something comes into the results channel it will read from it 
and put it into a hashtable for deduping. 

The problem is the deduping function is blocking, so when it's done you need to send a signal back to main that you're done
so to do that you add a function called wait to the commandmanager, and then send an empty struct into it when the results
channel is empty. All in WaitForDeDupingToFinish() 




error handling
1. you want a robust system whereby if one channel / workder causes a fuss, the rest of teh app continues to process the other channels. You do this with a couple of mechanislm
2. stderr - errors are captured through the centralized ErrorAggregator which logs all errors with context (severity, channel, component, etc.)
3. cmd.wiat and start returns errors. The go style is to return error objects that can be nil or not. This pattern is used for external intereactions eg file creation, network ops, db ops etc
so because you are using exec to launch a new process for yt-dlps, each one can have an error returned. You check that error in the main worker loop (which maps to one yt-dlp processs)
see the claen up section of the worker func. That will end the gourptine grracefully and (hopefully) also shut down the yt-dlp orocess. 

Context 
*/

package main 

import (
    "runtime"
	"fmt"

    "os"
	"io"
	"log"
    "time"
    "context"
    "io/ioutil"
    "strings"
    // "bytes"
    // "bufio"
    // "encoding/json"
    // "encoding/csv"
    // "os/signal"
    // "syscall"
    // "strconv"
    // "net/http"
    // "strconv"
    // "sync"
    // "os/exec"
    // _ "net/http/pprof" // Import for side effects
)

const pathOfCaptionsAlreadyDownloaded   = "./output_captions"
const numWorkersforChannels             = 5
const listofchannelstoscrape            = "video_sources.csv"
const ratelimitpersec                   = 0.02
const ratelimitburst                    = 5
const ytdlpDumpJSON                     = "--dump-json"
const ytdlpNoWarnings                   = "--no-warnings"
const ytdlpCookiesFile                  = "cookies.txt"
const ytdlp_cookiesparam string         = "--cookies"
var ytdlpDateAfter string              = "--dateafter"
var ytdlp_version string                = ""


func main() {
    // fmt.Printf("Active goroutines: %d\n", runtime.NumGoroutine())
    // debugGoroutines()

    // runds the pprof goroutine debugger - 
    // go func() {
    //     log.Println("Starting pprof goroutine debugger on server on localhost:6060")
    //     log.Println(http.ListenAndServe("localhost:6060", nil))
    // }()

    environment := os.Getenv("GO_ENV")  
    fmt.Println (environment)
    if (environment == "development") {
        // ytdlp_cookiesparam  = "--cookies-from-browser"
        // ytdlp_cookiesparam  = "--cookies"
        ytdlp_version       = "yt-dlp_macos"
    } else {
        // ytdlp_cookiesparam = "--cookies"
        ytdlp_version       = "yt-dlp"
    }



    // APPLICAITON LEVEL LOGGING SET UP 
    // Write all log.Printlns to the app.log file 
    file, err := os.OpenFile("logs/application_logs/app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
    if err != nil {
        // Can't log to file, but we can still log to stdout and continue
        log.Printf("WARNING: Failed to open log file: %v. Continuing with stdout logging only.", err)
    } else {
        defer file.Close()
        // Create a multi-writer to write application to both file and stdout 
        multiWriter := io.MultiWriter(os.Stdout, file)
        log.SetOutput(multiWriter)
    }
    


    // 1 ---- CENTRALIZED ERROR HANDLING AND LOGGING
    errorAggregator := NewErrorAggregator()
    defer errorAggregator.Shutdown()




    // 2 ---- CREATE APPLICATION-WIDE CONTEXT FOR GRACEFUL SHUTDOWN
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel() // Ensure context is cancelled on exit




    // // 3 ---- HANDLE SHUTDOWN SIGNALS (Ctrl+C, kill, etc.)
    // sigChan := make(chan os.Signal, 1)
    // signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    // go func() {
    //     sig := <-sigChan
    //     log.Printf("Received shutdown signal: %v. Initiating graceful shutdown...", sig)
    //     cancel() // Cancel context - all workers will stop
    // }()



    // 5 ---- CEATE COMMAND MANAGER FOR SCRAPING USING GOROUTINES - FEED IT THE CHANNELS FROM THE CSV
    manager, err := NewCommandManager(ctx, listofchannelstoscrape, errorAggregator,numWorkersforChannels,ratelimitpersec,ratelimitburst)
    if err != nil {
        errorAggregator.RecordError(SeverityCritical, -1, "", "", "command-manager", err, "Failed to create command manager")
        // log.Printf("CRITICAL: Failed to create command manager: %v", err)
        // Don't crash - print summary and exit gracefully
        errorAggregator.PrintSummary()
        return
    }
    // for record := range manager.commands {
    //     log.Println("Channel Scraping:", record)
    // }


    log.Println("starting")

    // 6 ---- PREVENT DUPLICATES load up all the captions for videos you already have so you don't download srt caption files for ones you already have 
    //right the already downloaded srt caption files are just coming from the file directory 
    files, err  := ioutil.ReadDir(pathOfCaptionsAlreadyDownloaded)
    if err != nil {
        errorAggregator.RecordError(SeverityWarning, -1, "", "", "file-io", err, "Failed to read output_captions directory - continuing without deduplication")
    }

    for _, file := range files {
        fileName := file.Name()
        if fileName == ".DS_Store" {
            continue
        }
        // File name format: "Channel___VideoID___Timestamp"
        parts := strings.Split(fileName, "___")
        if len(parts) != 3 {
            log.Printf("Warning: Unexpected filename format (expected Channel___VideoID___Timestamp): %s", fileName)
            continue
        }

        videoID := parts[1]
        manager.video_captions[videoID] = VideoToBeDownloadedResult{} // put empty object as you only need the id of the video to dedupe
    }
    log.Println("VIDEOS WITH CAPTIONS ALREADY DOWNLOADED. HASHMAP SEEDED FROM output_captions with:" , len(manager.video_captions))
    



    // 7 ---- Start the manager
    manager.Start()
    manager.WaitForAllWorkToFinish()

    log.Println("All yt-dlp workers completed. Starting caption downloads...")


    // 8 ---- START DOWNLOADING CAPTIONS
    captionDownloader := NewCaptionDownloadManager(ctx, errorAggregator,numWorkersforChannels,ratelimitpersec,ratelimitburst)
    manager.makeResultsInHashMapAvailableToParameterChannel(captionDownloader.CaptionsToBeDownloaded)
    captionDownloader.Start()
    
    log.Println("All caption downloads completed.")
    
    // Print error summary
    // errorAggregator.PrintSummary()
    
    log.Println("Application Done")



    /* this part is to be replaced some how by the DeDupeResultChanVideos() which has another channel "done" to signal completion of deduping process */
    // this is the main loop where you check the resuls channel. Everytime the workers from CommandWorker() run, 
    // they insert things into the results channel in their own goroutines. You check the output here. 

    // for {
    //     select {
    //     case result, ok := <-manager.resultChan: //read from the results channel as video urls come into the channel from reading youtube  videochannel 
    //         if ok == false {  // This checks to see if channel is closed 
    //             log.Println(/*time.Now().Format("15:04:05.000"),*/"Main results channel closed")
    //             return 
    //         }
    //         log.Println(result)

    //         if _, exists := video_captions[result.Id]; exists {
    //             // video already has subtitles extracted or queue to be extracted; ignore it
    //             // OPTIMIZATION - COULD WE HAVE A ROLLING WINDOW TO EXCLUDE THIS VIDEO FROM CHANNEL JSON DUMPS? MAYBE HAVE A MOVING TERMINATION DATE
    //         } else {
    //             video_captions[result.Id] = result

    //             //1 need to get the captions in goroutine
    //             //2 write them to a file 
    //             //3 remove the entry from the map 
    //             //4 persist vido to map/csv file of all downloaded srts 
    //             //0 load all downalod
    //             getCaptionWorkerCounter++
    //             go func (r VideoToBeDownloadedResult, workerCounterID int) {
    //                 log.Println(r.Originalurl," : Get Captions GoRoutine Started. Num of Goroutines:", runtime.NumGoroutine(), "SRT Caption WorkerID",workerCounterID)
    //                 defer log.Println(r.Originalurl," : Get Captions GoRoutine ENDED. Num of Goroutines:", runtime.NumGoroutine(), "SRT Caption WorkerID",workerCounterID)

    //                 err := getVideoCaptionsSaveToFile(r)
    //                 if err != nil {
    //                     log.Println("HTTP Get Caption Failed",err)
    //                 }

    //                 delete(video_captions,r.Id)
    //             }(result,getCaptionWorkerCounter)

                
    //         }
    //     case <- manager.ctx.Done():
    //         log.Println("main thread done")
    //         return 
    //     }
    // }

    // fmt.Printf("Final goroutine count: %d\n", runtime.NumGoroutine())

}


func debugGoroutines() {
    ticker := time.NewTicker(1 * time.Second)
    go func() {
        for {
            select {
            case <-ticker.C:
                fmt.Printf("Active goroutines: %d\n", runtime.NumGoroutine())
            }
        }
    }()
}



// OBSOLETE - references deleted videolisttxt constant
// func getVideoListToDownLoadTranscriptsFor() {
//     // Open the CSV file
//     file, err := os.Open(videolisttxt)
//     if err != nil { log.Fatal(err)}
//     defer file.Close()

//     scanner := bufio.NewScanner(file)

//     // Initialize the hashtable (map) to store the data
//     videoListHashTable := make(map[string]string)

//     for scanner.Scan() {
//             line := scanner.Text()
//             fields := strings.Split(line, ";")

//             // Assuming column 0 = name and the rest of the columns are stored as value
//             id              := fields[2]
//             videoName       := fields[1]
//             videoListHashTable[id]   = videoName
//     }

//     // fmt.Println(videoListHashTable)
// }

































/*
    OBSOLETE - this is no longer needed because you havea a more advanced command manager design 
    but this allows you to see how to directly manipulate yt-dlp as a process in the command line environment via golang's exec library 
    it hooks up your app to the stdout and stderr streams and has the business logic, without goroutines
*/

// func downloadChannelJson(vidparam string){
//     cmd := exec.Command("yt-dlp_macos","--dump-json", "--no-warnings", vidparam)
//     // cmd := exec.Command("yt-dlp_macos","--dump-json", vidparam)

//     stdout, err := cmd.StdoutPipe()
//     if err != nil {
//         log.Println("downloadjson", err)
//     }

//     // Start the command
//     if err := cmd.Start(); err != nil {
//         // log.Fatal(err)
//         log.Println("downloadjson cmd.Start()", err)
//     }

//     decoder := json.NewDecoder(stdout)

//     // Read and decode JSON objects incrementally
//     for {
//         var obj map[string]interface{} // Use a map to hold the JSON data
//         if err := decoder.Decode(&obj); err == io.EOF {
//             break // End of input
//         } else if err != nil {
//             log.Fatal("Error decoding JSON:", err)
//         }

//         // Process the decoded JSON object
//         // fmt.Printf("Processed object: %+v\n", obj)
//         uploader            := obj["uploader"].(string)
//         video_url           := obj["original_url"].(string)
//         id                  := obj["id"].(string)
//         title               := obj["title"].(string)
//         upload_date         := obj["upload_date"].(string)
//         captions            := obj["automatic_captions"].(map[string]interface{})
        
//         fmt.Println(uploader, id,video_url, title, upload_date)
        
//         if len(captions) > 0 {
//             englishcaptions := captions["en"].([]interface{})
//             for _, engCaptionURL := range englishcaptions {
//                 engC := engCaptionURL.(map[string]interface{})
//                 ext := engC["ext"].(string)
//                 if ext == "srt" {
//                     url := engC["url"].(string)
//                     fmt.Println(url)
//                 }
//             }
//         }

//         // jsonData, _ := json.MarshalIndent(englishcaptions,""," ")

//         // fmt.Println(string(jsonData))
//     }
  
//     // Wait for the command to finish
//     if err := cmd.Wait(); err != nil {
//         log.Fatal(err)
//     }
// }



//OBSOLETE -- now getting the full json dump instead so no need print out video list using commands
// i actually think the yt-dlp gets the json and parses it to produce this command
/*
func downloadChannelVideoList(vidparam string){
    var waitGroup sync.WaitGroup

    // file := "https://www.youtube.com/watch?v=Vo8OBoIpXUU"
    fileyoutubehandle   := vidparam
    tempChannel         := strings.Split(fileyoutubehandle,"/")
    channelName         := tempChannel[len(tempChannel)-1]
    fmt.Println(channelName)


    // dateAfter := "20250501"
    // dateWithQuotes := "\"" + dateAfter + "\""

    // cmd := exec.Command("yt-dlp_macos","--write-auto-sub", "--skip-download", "--sub-lang", "en", "--convert-subs", "srt",file)
    cmd := exec.Command("yt-dlp_macos", "--get-title", "--get-id", "--date", "today-1weeks", fileyoutubehandle)
    // cmd := exec.Command("yt-dlp_macos", "--get-title", "--get-id" , fileyoutubehandle)

    stdout, err := cmd.StdoutPipe()
    if err != nil {
        log.Fatal(err)
    }

    // Create or open the output file
    file, err := os.Create(videolisttxt)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    // Start the command
    if err := cmd.Start(); err != nil {
        log.Fatal(err)
    }

    // Use a WaitGroup to wait for the goroutine to finish
    waitGroup.Add(1)

    // Goroutine to stream output and process it
    go func() {
        defer waitGroup.Done()
        scanner := bufio.NewScanner(stdout)

        var outputLines []string
        var i,j int = 0, 0 
        var titleAndID string = ""
        for scanner.Scan() {
            i++
            j++

            line := scanner.Text()
            fmt.Println(line)
            if i == 1 {
                titleAndID = line 
            } else {
                titleAndID = titleAndID + ";" + line    
            }
            
            
            if i < 2 {
                titleAndID = channelName + ";" + titleAndID
                continue 
            }
            
            outputLines = append(outputLines, titleAndID)
            i           = 0
            titleAndID  = ""

            // Print the line to the console
            // fmt.Println(outputLines)
        
            //debug code to stop yt-dlp after x retrivals from a channel 
            if j == 20 {
                break
            }
        }

        // Check for errors during scanning
        if err := scanner.Err(); err != nil {
            log.Fatal(err)
        }

        // Process the output to join multiple lines into one
        singleLineOutput := strings.Join(outputLines, "\n")
        
        // fmt.Println(singleLineOutput)

        // Write the processed output to the file
        if _, err := file.WriteString(singleLineOutput); err != nil {
            log.Fatal(err)
        }
    }()


    // Wait for the goroutine to finish
    waitGroup.Wait()

    // Terminate the yt-dlp process command
    // if err := cmd.Process.Kill(); err != nil {
    //     log.Fatal("Failed to kill yt-dlp_macos:", err)
    // }


    // Wait for the command to finish
    if err := cmd.Wait(); err != nil {
        log.Fatal(err)
    }
}
*/

/*package main

import (
    "bufio"
    "fmt"
    "os"
    "regexp"
    "encoding/json"
)

type Subtitle struct { 
	ID int `json:"id"` 
	Text string `json:"text"` 
}



func main() {
    inputFilePath := "Omaha Nebraska City Council meeting November 26, 2024 [Vo8OBoIpXUU].en.vtt"
    outputFilePath := "output.json"

    err := extractPlainText(inputFilePath, outputFilePath)
    if err != nil {
        fmt.Println("Error:", err)
    } else {
        fmt.Println("Plain text extracted successfully.")
    }
}

// code the extract txt from srt file? 
func extractPlainText(inputFilePath, outputFilePath string) error {
    file, err := os.Open(inputFilePath)
    if err != nil {
        return err
    }
    defer file.Close()

    outputFile, err := os.Create(outputFilePath)
    if err != nil {
        return err
    }
    defer outputFile.Close()

    scanner := bufio.NewScanner(file)
    writer := bufio.NewWriter(outputFile)
    defer writer.Flush()

    timestampPattern := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}`)
    htmlTagPattern := regexp.MustCompile(`<.*?>`)

    var id int

    writer.WriteString("[")
    for scanner.Scan() {
        line := scanner.Text()

        // Skip timestamp lines
        if timestampPattern.MatchString(line) {
            continue
        }

        // Remove HTML tags
        line = htmlTagPattern.ReplaceAllString(line, "")
        if line == " " {
            continue
        }
        if len(line) > 0 {
            id++
            subtitle := Subtitle{ ID: id, Text: line, }
            jsonLine, err := json.Marshal(subtitle)
            if err != nil { 
                return err 
            }
            writer.WriteString(string(jsonLine) + ",\n")
        }
    }
    writer.WriteString("]")

    if err := scanner.Err(); err != nil {
        return err
    }

    return nil
}*/