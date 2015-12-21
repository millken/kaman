package file

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bbangert/toml"
	"github.com/cactus/gostrftime"
	"github.com/millken/kaman/plugins"
)

type outBatch struct {
	data   []byte
	cursor string
}

func newOutBatch() *outBatch {
	return &outBatch{
		data: make([]byte, 0, 10000),
	}
}

type FileOutputConfig struct {
	// Full output file path.
	// If date rotation is in use, then the output file name can support
	// Go's time.Format syntax to embed timestamps in the filename:
	// http://golang.org/pkg/time/#Time.Format
	Path string

	// Output file permissions (default "644").
	Perm string

	// Interval at which the output file should be rotated, in hours.
	// Only the following values are allowed: 0, 1, 4, 12, 24
	// The files will be named relative to midnight of the day.
	// (default 0, i.e. disabled). Set to 0 to disable.
	RotationInterval uint32 `toml:"rotation_interval"`

	// Interval at which accumulated file data should be written to disk, in
	// milliseconds (default 1000, i.e. 1 second). Set to 0 to disable.
	FlushInterval uint32 `toml:"flush_interval"`

	// Permissions to apply to directories created for FileOutput's parent
	// directory if it doesn't exist.  Must be a string representation of an
	// octal integer. Defaults to "700".
	FolderPerm string `toml:"folder_perm"`
}

type FileOutput struct {
	common     *plugins.PluginCommonConfig
	config     *FileOutputConfig
	path       string
	perm       os.FileMode
	file       *os.File
	batchChan  chan *outBatch
	backChan   chan *outBatch
	folderPerm os.FileMode
	timerChan  <-chan time.Time
	rotateChan chan time.Time
	closing    chan struct{}
}

func (self *FileOutput) Init(pcf *plugins.PluginCommonConfig, conf toml.Primitive) error {
	var err error
	var intPerm int64
	log.Println("FileOutput Init.")
	self.common = pcf
	self.config = &FileOutputConfig{
		Perm:             "644",
		RotationInterval: 0,
		FlushInterval:    1000,
		FolderPerm:       "700",
	}
	if err := toml.PrimitiveDecode(conf, self.config); err != nil {
		return fmt.Errorf("Can't unmarshal FileOutput config: %s", err)
	}
	if intPerm, err = strconv.ParseInt(self.config.FolderPerm, 8, 32); err != nil {
		err = fmt.Errorf("FileOutput '%s' can't parse `folder_perm`, is it an octal integer string?",
			self.config.Path)
		return err
	}
	self.folderPerm = os.FileMode(intPerm)

	if intPerm, err = strconv.ParseInt(self.config.Perm, 8, 32); err != nil {
		err = fmt.Errorf("FileOutput '%s' can't parse `perm`, is it an octal integer string?",
			self.config.Path)
		return err
	}
	self.perm = os.FileMode(intPerm)
	self.closing = make(chan struct{})
	switch self.config.RotationInterval {
	case 0:
		// date rotation is disabled
		self.path = self.config.Path
	case 1, 4, 12, 24:
		// RotationInterval value is allowed
		self.startRotateNotifier()
	default:
		err = fmt.Errorf("Parameter 'rotation_interval' must be one of: 0, 1, 4, 12, 24.")
		return err
	}
	if err = self.openFile(); err != nil {
		err = fmt.Errorf("FileOutput '%s' error opening file: %s", self.path, err)
		close(self.closing)
		return err
	}

	self.batchChan = make(chan *outBatch)
	self.backChan = make(chan *outBatch, 2) // Never block on the hand-back
	self.rotateChan = make(chan time.Time)
	return err
}

func (self *FileOutput) openFile() (err error) {
	basePath := filepath.Dir(self.path)
	if err = os.MkdirAll(basePath, self.folderPerm); err != nil {
		return fmt.Errorf("Can't create the basepath for the FileOutput plugin: %s", err.Error())
	}
	self.file, err = os.OpenFile(self.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, self.perm)
	return
}

func (self *FileOutput) startRotateNotifier() {
	now := time.Now()
	interval := time.Duration(self.config.RotationInterval) * time.Hour
	last := now.Truncate(interval)
	next := last.Add(interval)
	until := next.Sub(now)
	after := time.After(until)

	self.path = gostrftime.Strftime(self.config.Path, now)

	go func() {
		ok := true
		for ok {
			select {
			case _, ok = <-self.closing:
				break
			case <-after:
				last = next
				next = next.Add(interval)
				until = next.Sub(time.Now())
				after = time.After(until)
				self.rotateChan <- last
			}
		}
	}()
}

func (self *FileOutput) Run(runner plugins.OutputRunner) error {
	errChan := make(chan error, 1)
	go self.committer(runner, errChan)
	return self.receiver(runner, errChan)
}

func (self *FileOutput) receiver(runner plugins.OutputRunner, errChan chan error) (err error) {
	var (
		pack          *plugins.PipelinePack
		timer         *time.Timer
		timerDuration time.Duration
		msgCounter    uint32
		outBytes      []byte
	)
	ok := true
	out := newOutBatch()
	inChan := runner.InChan()

	timerDuration = time.Duration(self.config.FlushInterval) * time.Millisecond
	if self.config.FlushInterval > 0 {
		timer = time.NewTimer(timerDuration)
		if self.timerChan == nil { // Tests might have set this already.
			self.timerChan = timer.C
		}
	}

	for ok {
		select {
		case pack = <-inChan:
			pack, err = plugins.PipeDecoder(self.common.Decoder, pack)
			if err != nil {
				log.Printf("PipeDecoder :%s", err)
				pack.Recycle()
				continue
			}
			pack, err = plugins.PipeEncoder(self.common.Encoder, pack)
			if err != nil {
				log.Printf("PipeEncoder :%s", err)
				pack.Recycle()
				continue
			}
			outBytes = pack.Msg.MsgBytes

			if outBytes != nil {
				out.data = append(out.data, outBytes...)
				out.data = append(out.data, '\n')
				msgCounter++
			}
			pack.Recycle()

		case <-self.timerChan:
			// This will block until the other side is ready to accept
			// this batch, freeing us to start on the next one.
			self.batchChan <- out
			out = <-self.backChan
			msgCounter = 0
			timer.Reset(timerDuration)
		case err = <-errChan:
			ok = false
			break
		}
	}
	return err
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, writes it out to the filesystem, and puts the now empty buffer on
// the return channel for reuse.
func (self *FileOutput) committer(or plugins.OutputRunner, errChan chan error) {
	initBatch := newOutBatch()
	self.backChan <- initBatch
	var out *outBatch
	var err error

	ok := true

	for ok {
		select {
		case out, ok = <-self.batchChan:
			if !ok {
				// Channel is closed => we're shutting down, exit cleanly.
				self.file.Close()
				close(self.closing)
				break
			}
			n, err := self.file.Write(out.data)
			if err != nil {
				log.Println(fmt.Errorf("Can't write to %s: %s", self.path, err))
			} else if n != len(out.data) {
				log.Println(fmt.Errorf("data loss - truncated output for %s", self.path))
			} else {
				self.file.Sync()
			}
			out.data = out.data[:0]
			self.backChan <- out
		case rotateTime := <-self.rotateChan:
			self.file.Close()
			self.path = gostrftime.Strftime(self.config.Path, rotateTime)
			if err = self.openFile(); err != nil {
				close(self.closing)
				err = fmt.Errorf("unable to open rotated file '%s': %s", self.path, err)
				errChan <- err
				ok = false
				break
			}
		}
	}
}

func init() {
	plugins.RegisterOutput("FileOutput", func() interface{} {
		return new(FileOutput)
	})
}
