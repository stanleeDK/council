package main

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"time"
)

// logTimestampLayout matches the prefix written by the standard log package
// (log.LstdFlags), e.g. "2026/06/25 14:31:53".
const logTimestampLayout = "2006/01/02 15:04:05"

// pruneOldLogLines rewrites the log file at path, dropping any line whose leading
// timestamp is older than maxAge. Lines without a parseable leading timestamp
// (e.g. continuation lines of a multi-line message) inherit the keep/drop
// decision of the most recent timestamped line. A missing file is a no-op.
//
// The rewrite is atomic (temp file in the same dir + rename), so an interrupted
// prune can never corrupt or truncate the log. It does NOT touch caption files.
func pruneOldLogLines(path string, maxAge time.Duration) {
	in, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("log prune: could not open %s: %v", path, err)
		}
		return // nothing to prune (e.g. first run)
	}
	defer in.Close()

	cutoff := time.Now().Add(-maxAge)

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".prune-*")
	if err != nil {
		log.Printf("log prune: could not create temp file for %s: %v", path, err)
		return
	}
	tmpName := tmp.Name()
	w := bufio.NewWriter(tmp)

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024) // allow long log lines (commands, error bodies)

	keep := true // default: keep any leading lines before the first timestamp
	removed := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) >= len(logTimestampLayout) {
			if ts, perr := time.Parse(logTimestampLayout, string(line[:len(logTimestampLayout)])); perr == nil {
				keep = !ts.Before(cutoff) // keep when timestamp >= cutoff
			}
		}
		if keep {
			w.Write(line)
			w.WriteByte('\n')
		} else {
			removed++
		}
	}

	if serr := scanner.Err(); serr != nil {
		log.Printf("log prune: error reading %s: %v", path, serr)
		tmp.Close()
		os.Remove(tmpName)
		return
	}
	if err := w.Flush(); err != nil {
		log.Printf("log prune: error writing temp for %s: %v", path, err)
		tmp.Close()
		os.Remove(tmpName)
		return
	}
	if err := tmp.Close(); err != nil {
		log.Printf("log prune: error closing temp for %s: %v", path, err)
		os.Remove(tmpName)
		return
	}

	in.Close() // release the read handle before replacing
	if err := os.Rename(tmpName, path); err != nil {
		log.Printf("log prune: could not replace %s: %v", path, err)
		os.Remove(tmpName)
		return
	}
	if removed > 0 {
		log.Printf("log prune: removed %d line(s) older than %s from %s", removed, maxAge, path)
	}
}
