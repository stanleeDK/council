package main

import "time"

/*
    A struct to hold all info about a channel that will be hit to get a list of videos
*/

type ChannelToBeScraped struct {
    Platform                        string
    Channel                         string
    Url                             string
    Command                         string
    Args                            []string
    YoungestVideoUploaded_at        time.Time
    IsRecentlyAdded                 string // "true" or "false" if true then scrape past year of history
}

/*
    yt-dlp when given a channel url will return lists of videos (via jsondump parse)
    this struct will hold individual videos to have their subtitles downloaded
*/
type VideoToBeDownloadedResult struct {
    WorkerID int
    Channel     string
    Upload_date time.Time
    Id          string
    Captionurl  string
    Originalurl string

}
