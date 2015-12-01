package file

import (
	//"encoding/binary"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/bbangert/toml"
	"github.com/hpcloud/tail"
	"github.com/millken/kaman/plugins"
)

type TailsInputConfig struct {
	// Log base directory to run log regex under
	LogDirectory string `toml:"log_directory"`
	// Journal base directory for saving journal files
	JournalDirectory string `toml:"journal_directory"`
	// File match for regular expression
	FileMatch      string `toml:"file_match"`
	SyncInterval   int    `toml:"sync_interval"`
	RescanInterval string `toml:"rescan_interval"`
}

type TailsInput struct {
	config         *TailsInputConfig
	common         *plugins.PluginCommonConfig
	rescanInterval time.Duration
	files          []string
	runner         plugins.InputRunner
}

// Represents an individual Logfile which is part of a Logstream
type Logfile struct {
	FileName string
	// The raw string matches from the filename, keys being the strings, values
	// are the portion that was matched
	StringMatchParts map[string]string
	// The matched portions of the filename and their translated integer value
	// MatchParts maps to integers used for sorting the Logfile within the
	// Logstream
	MatchParts map[string]int
}

// Alias for a slice of logfiles, used mainly for sorting algorithms
type Logfiles []*Logfile

// Implement two of the sort.Interface methods needed
func (l Logfiles) Len() int      { return len(l) }
func (l Logfiles) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Locate the index of a given filename if its present in the logfiles for this stream
// Returns -1 if the filename is not present
func (l Logfiles) IndexOf(s string) int {
	for i := 0; i < len(l); i++ {
		if l[i].FileName == s {
			return i
		}
	}
	return -1
}

// Returns a list of all the filenames in their current order
func (l Logfiles) FileNames() []string {
	s := make([]string, len(l))
	for _, logfile := range l {
		s = append(s, logfile.FileName)
	}
	return s
}

// Returns a Logfiles only containing logfiles newer than oldTime
// based on the files last modified file attribute
func (l Logfiles) FilterOld(oldTime time.Time) Logfiles {
	f := make(Logfiles, 0)
	for _, logfile := range l {
		finfo, err := os.Stat(logfile.FileName)
		if err != nil {
			continue
		}
		if finfo.ModTime().After(oldTime) {
			f = append(f, logfile)
		}
	}
	return f
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// append a path separator if needed and escape regexp meta characters
func fileMatchRegexp(logRoot, fileMatch string) *regexp.Regexp {
	if !os.IsPathSeparator(logRoot[len(logRoot)-1]) && !os.IsPathSeparator(fileMatch[0]) {
		logRoot += string(os.PathSeparator)
	}
	return regexp.MustCompile("^" + regexp.QuoteMeta(logRoot) + fileMatch)
}

// Scans a directory recursively filtering out files that match the fileMatch regexp
func ScanDirectoryForLogfiles(directoryPath string, fileMatch *regexp.Regexp) Logfiles {
	files := make(Logfiles, 0)
	filepath.Walk(directoryPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if fileMatch.MatchString(path) {
			files = append(files, &Logfile{FileName: path})
		}
		return nil
	})
	return files
}

func (this *TailsInput) ExistInQueue(file string) bool {
	for _, logfile := range this.files {
		if file == logfile {
			return true
		}
	}
	return false
}
func (this *TailsInput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) (err error) {
	this.common = pcf
	this.config = &TailsInputConfig{
		LogDirectory:     "/var/log",
		JournalDirectory: "/tmp/",
		//FileMatch: "*.log",
		RescanInterval: "1m",
		SyncInterval:   2,
	}
	this.files = make([]string, 0)
	if err := toml.PrimitiveDecode(conf, this.config); err != nil {
		return fmt.Errorf("Can't unmarshal tails config: %s", err)
	}
	if this.config.FileMatch == "" {
		return errors.New("`file_match` setting is required.")
	}
	if len(this.config.FileMatch) > 0 && this.config.FileMatch[len(this.config.FileMatch)-1:] != "$" {
		this.config.FileMatch += "$"
	}
	// Setup the rescan interval.
	if this.rescanInterval, err = time.ParseDuration(this.config.RescanInterval); err != nil {
		return
	}
	if !fileExists(this.config.JournalDirectory) {
		if err = os.MkdirAll(filepath.Dir(this.config.JournalDirectory), 0766); err != nil {
			return
		}
	}
	return nil
}

func (this *TailsInput) Watcher() {
	fileMatch := fileMatchRegexp(this.config.LogDirectory, this.config.FileMatch)
	// Scan for all our logfiles
	logfiles := ScanDirectoryForLogfiles(this.config.LogDirectory, fileMatch)
	for _, logfile := range logfiles {
		if this.ExistInQueue(logfile.FileName) {
			continue
		}
		this.files = append(this.files, logfile.FileName)
		log.Printf("%s", logfile.FileName)
		go this.Tailer(logfile.FileName)
	}

}

func writePoint(filename string, offset int64) (err error) {
	var pointFile *os.File
	if pointFile, err = os.OpenFile(filename,
		os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer pointFile.Close()
	pointFile.Seek(0, 0)
	err = binary.Write(pointFile, binary.LittleEndian, &offset)
	return
}

func readPoint(filename string) (offset int64, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	err = binary.Read(file, binary.LittleEndian, &offset)
	return
}

func (this *TailsInput) Tailer(f string) error {
	var seek int
	var offset int64
	var err error
	pointfile := fmt.Sprintf("%s/%d", this.config.JournalDirectory, hash(f))
	if offset, err = readPoint(pointfile); err != nil {
		seek = os.SEEK_END
	} else {
		seek = os.SEEK_SET
	}
	t, err := tail.TailFile(f, tail.Config{
		Poll:      true,
		ReOpen:    true,
		Follow:    true,
		MustExist: false,
		Location:  &tail.SeekInfo{int64(offset), seek},
	})
	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second * time.Duration(3))
	count := 0

	for {
		select {
		case <-tick.C:
			{
				if count > 0 {
					offset, err := t.Tell()
					if err != nil {
						log.Println("Tell return error: ", err)
						continue
					}
					if err = writePoint(pointfile, offset); err != nil {
						return err
					}
					count = 0
				}
			}
		case line := <-t.Lines:
			{
				pack := <-this.runner.InChan()
				pack.MsgBytes = []byte(line.Text)
				pack.Msg.Tag = this.common.Tag
				pack.Msg.Timestamp = time.Now().Unix()
				count++
				this.runner.RouterChan() <- pack
			}
		}
	}
	err = t.Wait()
	return err
}

func (this *TailsInput) Run(runner plugins.InputRunner) (err error) {
	defer func() {

		if err := recover(); err != nil {
			log.Fatalln("recover panic at err:", err)
		}
	}()
	this.runner = runner
	this.Watcher()
	ok := true
	rescan := time.Tick(this.rescanInterval)
	for ok {
		select {
		case <-rescan:
			{
				this.Watcher()
			}
		}
	}
	return nil
}

func init() {
	plugins.RegisterInput("TailsInput", func() interface{} {
		return new(TailsInput)
	})
}
