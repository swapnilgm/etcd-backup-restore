// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package snapshotter

import (
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

const (
	// GarbageCollectionPolicyExponential defines the exponential policy for garbage collecting old backups
	GarbageCollectionPolicyExponential = "Exponential"
	// GarbageCollectionPolicyLimitBased defines the limit based policy for garbage collecting old backups
	GarbageCollectionPolicyLimitBased = "LimitBased"

	// SnapshotterStateInactive indicates that snapshotter is currently not running.
	SnapshotterStateInactive uint32 = 0
	// SnapshotterStateActive indicates that snapshotter is currently running.
	SnapshotterStateActive uint32 = 1
)

// Snapshotter is a struct for etcd snapshot taker
type Snapshotter struct {
	config              *Config
	logger              *logrus.Logger
	prevSnapshot        *snapstore.Snapshot
	wg                  sync.WaitGroup
	garbageCollectionWg sync.WaitGroup
	fullSnapshotCh      chan struct{}
	deltaStopCh         chan struct{}
	StateMutex          sync.Mutex
	State               uint32
	fullSnapshotTimer   *time.Timer
}

// Config stores the configuration parameters for the snapshotter.
type Config struct {
	schedule                       cron.Schedule
	store                          snapstore.SnapStore
	maxBackups                     int
	deltaSnapshotIntervalSeconds   int
	etcdConnectionTimeout          time.Duration
	garbageCollectionPeriodSeconds time.Duration
	garbageCollectionPolicy        string
	tlsConfig                      *TLSConfig
}

// TLSConfig holds cert information and settings for TLS.
type TLSConfig struct {
	cert       string
	key        string
	caCert     string
	insecureTr bool
	skipVerify bool
	endpoints  []string
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}
