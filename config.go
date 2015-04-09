/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Victor Ng (vng@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

// Hekad configuration.

package main

import (
	"fmt"
	"github.com/bbangert/toml"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type MasterConfig struct {
	Maxprocs              int           `toml:"maxprocs"`
	PoolSize              int           `toml:"poolsize"`
	ChanSize              int           `toml:"plugin_chansize"`
	CpuProfName           string        `toml:"cpuprof"`
	MemProfName           string        `toml:"memprof"`
	MaxMsgLoops           uint          `toml:"max_message_loops"`
	MaxMsgProcessInject   uint          `toml:"max_process_inject"`
	MaxMsgProcessDuration uint64        `toml:"max_process_duration"`
	MaxMsgTimerInject     uint          `toml:"max_timer_inject"`
	MaxPackIdle           time.Duration `toml:"max_pack_idle"`
	BaseDir               string        `toml:"base_dir"`
	ShareDir              string        `toml:"share_dir"`
	SampleDenominator     int           `toml:"sample_denominator"`
	PidFile               string        `toml:"pid_file"`
	Hostname              string
	MaxMessageSize        uint32 `toml:"max_message_size"`
}

func ReplaceEnvsFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func LoadConfig(configPath string) (masterConfig *MasterConfig, plugConfig map[string]toml.Primitive, err error) {
	idle, _ := time.ParseDuration("2m")
	hostname, err := os.Hostname()
	if err != nil {
		return
	}

	masterConfig = &MasterConfig{Maxprocs: 1,
		PoolSize:              100,
		ChanSize:              30,
		CpuProfName:           "",
		MemProfName:           "",
		MaxMsgLoops:           4,
		MaxMsgProcessInject:   1,
		MaxMsgProcessDuration: 100000,
		MaxMsgTimerInject:     10,
		MaxPackIdle:           idle,
		BaseDir:               filepath.FromSlash("/var/cache/hekad"),
		ShareDir:              filepath.FromSlash("/usr/share/heka"),
		SampleDenominator:     1000,
		PidFile:               "",
		Hostname:              hostname,
	}

	var configFile map[string]toml.Primitive
	p, err := os.Open(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("Error opening config file: %s", err)
	}
	fi, err := p.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("Error fetching config file info: %s", err)
	}

	if fi.IsDir() {
		files, _ := ioutil.ReadDir(configPath)
		for _, f := range files {
			fName := f.Name()
			if !strings.HasSuffix(fName, ".toml") {
				// Skip non *.toml files in a config dir.
				continue
			}
			fPath := filepath.Join(configPath, fName)
			contents, err := ReplaceEnvsFile(fPath)
			if err != nil {
				return nil, nil, err
			}
			if _, err = toml.Decode(contents, &configFile); err != nil {
				return nil, nil, fmt.Errorf("Error decoding config file: %s", err)
			}
		}
	} else {
		contents, err := ReplaceEnvsFile(configPath)
		if err != nil {
			return nil, nil, err
		}
		if _, err = toml.Decode(contents, &configFile); err != nil {
			return nil, nil, fmt.Errorf("Error decoding config file: %s", err)
		}
	}

	empty_ignore := map[string]interface{}{}
	parsed_config, ok := configFile["master"]
	if ok {
		if err = toml.PrimitiveDecodeStrict(parsed_config, masterConfig, empty_ignore); err != nil {
			err = fmt.Errorf("Can't unmarshal master config: %s", err)
		}
	}
	plugConfig = configFile
	delete(plugConfig, "master")
	return
}
