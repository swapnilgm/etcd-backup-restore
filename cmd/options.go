// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"io"
	"io/ioutil"

	"github.com/gardener/etcd-backup-restore/pkg/config"
	"github.com/ghodss/yaml"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

type options struct {
	ConfigFile string
	Version    bool
	StdOut     io.Writer
	StdErr     io.Writer
	LogLevel   int
	Logger     *logrus.Logger
	Config     *config.BackupRestoreComponentConfig
}

// newOptions returns a new Options object.
func newOptions(out, errOut io.Writer) *options {
	Logger := logrus.New()
	Logger.SetLevel(logrus.InfoLevel)
	Logger.Out = out
	Logger.Formatter = logrus.TextFormatter

	return &options{
		StdOut:   out,
		StdErr:   errOut,
		LogLevel: 4,
		Version:  false,
		Config:   config.NewBackupRestoreComponentConfig(),
		Logger:   logger,
	}
}

func (o *options) validate() error {
	return o.Config.Validate()
}

func (o *options) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config-file", o.ConfigFile, "path to the configuration file")
	fs.StringVar(&o.LogLevel, "log-level", o.LogLevel, "verbosity level of logs")
	o.Config.AddFlags(fs)
}

func (o *options) complete() {
	Logger.SetLevel(o.LogLevel)
}

func (o *options) loadFromConfigFile(fs *flag.FlagSet) error {
	if len(o.ConfigFile) != 0 {
		data, err := ioutil.ReadFile(o.ConfigFile)
		if err != nil {
			return err
		}
		config := config.NewBackupRestoreComponentConfig()
		if err := yaml.Unmarshal(data, config); err != nil {
			return err
		}
	}
}

func (o *options) run() {

}
