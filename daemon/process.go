package daemon

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type UtilError string

func (err UtilError) Error() string { return "Utils Error: " + string(err) }

func ProcessName() string {
	file := os.Args[0]
	i := strings.LastIndex(file, "\\")
	j := strings.LastIndex(file, "/")
	if j < i {
		file = file[i+1:]
	} else if j > i {
		file = file[j+1:]
	}
	i = strings.LastIndex(file, ".")
	if i > 0 {
		file = file[0:i]
	}
	return file
}

func DefaultPidFileName() string {
	file := ProcessName()
	pidFile := "/var/run/"
	if runtime.GOOS == "windows" {
		pidFile = "c:\\" + file + ".pid"
	} else {
		pidFile += file + ".pid"
	}
	return pidFile
}

func ProcessFile() string {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	return path
}

func ProcessPath() string {
	path := filepath.Dir(ProcessFile())
	return path
}

func WritePidFile(myFile string, pid int) (err error) {
	return ioutil.WriteFile(myFile, []byte(fmt.Sprintf("%d", pid)), 0644)
}

func CheckWritePidPermission(pidFile string) error {
	if len(pidFile) <= 0 {
		pidFile = DefaultPidFileName()
	}
	file := pidFile + ".tmp"
	if err := ioutil.WriteFile(file, []byte(fmt.Sprintf("%d", 0)), 0644); err != nil {
		return UtilError("had no permission to write pid file")
	}
	os.Remove(file)
	return nil
}

func ExecProcess(background bool, file string, args ...string) (int, error) {
	fmt.Println("args:", args, "-Len:", len(args))
	filePath, _ := filepath.Abs(file)
	cmd := exec.Command(filePath, args...)
	if background {
		cmd.Stdin = nil 
		cmd.Stdout = nil
		cmd.Stderr = nil
	} else {
		cmd.Stdin = os.Stdin 
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	err := cmd.Start()
	if err == nil {
		return cmd.Process.Pid, nil
	}
	return -1, err
}

func TryToRunAsDaemon(key, pidFile string) int {
	key = strings.ToLower(key)
	var argsNew []string
	var bFind bool = false
	for _, arg := range os.Args[1:] {
		arg = strings.ToLower(arg)
		if arg == key {
			bFind = true
		} else {
			argsNew = append(argsNew, arg)
		}
	}
	if bFind {
		if err := CheckWritePidPermission(pidFile); err != nil {
			return -1
		}
		if pid, err := ExecProcess(true, ProcessFile(), argsNew...); err == nil {
			return pid
		}
		return -2
	}
	return 0
}

func StartProcess(background bool, file string, args []string) (*os.Process, error) {
	filePath, _ := filepath.Abs(file)
	if background {
		return os.StartProcess(filePath, args, &os.ProcAttr{Files: []*os.File{nil, nil, nil}})
	}
	return os.StartProcess(filePath, args, &os.ProcAttr{Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
}

func WaitProcess(background bool, file string, args []string) ([]byte, error) {
	filePath, _ := filepath.Abs(file)
	cmd := exec.Command(filePath, args...)
	if background {
		cmd.Stdin = nil 
		cmd.Stdout = nil
		cmd.Stderr = nil
	} else {
		cmd.Stdin = os.Stdin 
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	//cmd.Run()can not return body
	return cmd.Output()
}
