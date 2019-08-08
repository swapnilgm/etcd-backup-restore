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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"sync"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/metrics"

	"github.com/coreos/etcd/clientv3"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// NewSnapshotterConfig returns a config for the snapshotter.
func NewSnapshotterConfig(schedule string, store snapstore.SnapStore, maxBackups, deltaSnapshotIntervalSeconds, deltaSnapshotMemoryLimit int, etcdConnectionTimeout, garbageCollectionPeriodSeconds time.Duration, garbageCollectionPolicy string, tlsConfig *etcdutil.TLSConfig) (*Config, error) {
	logrus.Printf("Validating schedule...")
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}

	if garbageCollectionPolicy == GarbageCollectionPolicyLimitBased && maxBackups < 1 {
		logrus.Infof("Found garbage collection policy: [%s], and maximum backup value %d less than 1. Setting it to default: %d ", GarbageCollectionPolicyLimitBased, maxBackups, DefaultMaxBackups)
		maxBackups = DefaultMaxBackups
	}
	if deltaSnapshotIntervalSeconds < 1 {
		logrus.Infof("Found delta snapshot interval %d second less than 1 second. Disabling delta snapshotting. ", deltaSnapshotIntervalSeconds)
	}
	if deltaSnapshotMemoryLimit < 1 {
		logrus.Infof("Found delta snapshot memory limit %d bytes less than 1 byte. Setting it to default: %d ", deltaSnapshotMemoryLimit, DefaultDeltaSnapMemoryLimit)
		deltaSnapshotMemoryLimit = DefaultDeltaSnapMemoryLimit
	}

	return &Config{
		schedule:                       sdl,
		store:                          store,
		deltaSnapshotIntervalSeconds:   deltaSnapshotIntervalSeconds,
		deltaSnapshotMemoryLimit:       deltaSnapshotMemoryLimit,
		etcdConnectionTimeout:          etcdConnectionTimeout,
		garbageCollectionPeriodSeconds: garbageCollectionPeriodSeconds,
		garbageCollectionPolicy:        garbageCollectionPolicy,
		maxBackups:                     maxBackups,
		tlsConfig:                      tlsConfig,
	}, nil
}

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(ctx context.Context, logger *logrus.Logger, config *Config) *Snapshotter {
	// Create dummy previous snapshot
	var prevSnapshot *snapstore.Snapshot
	fullSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(ctx, config.store)
	if err != nil || fullSnap == nil {
		prevSnapshot = snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, 0)
	} else if len(deltaSnapList) == 0 {
		prevSnapshot = fullSnap
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	} else {
		prevSnapshot = deltaSnapList[len(deltaSnapList)-1]
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.CreatedOn.Unix()))
	}

	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: prevSnapshot.Kind}).Set(float64(prevSnapshot.LastRevision))
	return &Snapshotter{
		ctx:              ctx,
		logger:           logger,
		prevSnapshot:     prevSnapshot,
		PrevFullSnapshot: fullSnap,
		config:           config,
		SsrState:         SnapshotterInactive,
		SsrStateMutex:    &sync.Mutex{},
		fullSnapshotCh:   make(chan struct{}),
		cancelWatch:      func() {},
	}
}

// Run process loop for scheduled backup
// Setting <startWithFullSnapshot> to false will start the snapshotter without
// taking the first full snapshot. To stop the loop on should cancel the context passed while creating
// snapshotter.
func (ssr *Snapshotter) Run(startWithFullSnapshot bool) error {
	if startWithFullSnapshot {
		ssr.fullSnapshotTimer = time.NewTimer(0)
	} else {
		// for the case when snapshotter is run for the first time on
		// a fresh etcd with startWithFullSnapshot set to false, we need
		// to take the first delta snapshot(s) initially and then set
		// the full snapshot schedule
		if ssr.watchCh == nil {
			err := ssr.CollectEventsSincePrevSnapshot()
			if err != nil {
				ssr.logger.Errorf("Failed to collect events for first delta snapshot(s): %v", err)
				return err
			}
		}
		if err := ssr.resetFullSnapshotTimer(); err != nil {
			ssr.logger.Errorf("failed to reset full snapshot timer: %v", err)
			return err
		}
	}

	ssr.deltaSnapshotTimer = time.NewTimer(time.Duration(DefaultDeltaSnapshotIntervalSeconds))
	if ssr.config.deltaSnapshotIntervalSeconds >= 1 {
		ssr.deltaSnapshotTimer.Stop()
		ssr.deltaSnapshotTimer.Reset(time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	}

	defer ssr.stop()
	return ssr.snapshotEventHandler()
}

// TriggerFullSnapshot sends the events to take full snapshot. This is to
// trigger full snapshot externally out of regular schedule.
func (ssr *Snapshotter) TriggerFullSnapshot(ctx context.Context) error {
	ssr.SsrStateMutex.Lock()
	defer ssr.SsrStateMutex.Unlock()

	if ssr.SsrState != SnapshotterActive {
		return fmt.Errorf("snapshotter is not active")
	}
	ssr.logger.Info("Triggering out of schedule full snapshot...")
	ssr.fullSnapshotCh <- emptyStruct

	return nil
}

// stop stops the snapshotter. Once stopped any subsequent calls will
// not have any effect.
func (ssr *Snapshotter) stop() {
	ssr.SsrStateMutex.Lock()
	if ssr.fullSnapshotTimer != nil {
		ssr.fullSnapshotTimer.Stop()
		ssr.fullSnapshotTimer = nil
	}
	if ssr.deltaSnapshotTimer != nil {
		ssr.deltaSnapshotTimer.Stop()
		ssr.deltaSnapshotTimer = nil
	}
	ssr.closeEtcdClient()

	ssr.SsrState = SnapshotterInactive
	ssr.SsrStateMutex.Unlock()
}

func (ssr *Snapshotter) closeEtcdClient() {
	if ssr.cancelWatch != nil {
		ssr.cancelWatch()
	}
	if ssr.etcdClient != nil {
		if err := ssr.etcdClient.Close(); err != nil {
			ssr.logger.Warnf("Error while closing etcd client connection, %v", err)
		}
		ssr.etcdClient = nil
	}
}

// TakeFullSnapshotAndResetTimer takes a full snapshot and resets the full snapshot
// timer as per the schedule.
func (ssr *Snapshotter) TakeFullSnapshotAndResetTimer() error {
	ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
	if err := ssr.takeFullSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking scheduled snapshot failed: %v", err)
		return err
	}

	return ssr.resetFullSnapshotTimer()
}

// takeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) takeFullSnapshot() error {
	defer ssr.cleanupInMemoryEvents()
	// close previous watch and client.
	ssr.closeEtcdClient()

	client, err := etcdutil.GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		ssr.logger.Errorf("failed to create etcd client: %v", err)
		return err
	}

	connectionCtx, cancel := context.WithTimeout(ssr.ctx, ssr.config.etcdConnectionTimeout*time.Second)
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := client.Get(connectionCtx, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		ssr.logger.Errorf("failed to get etcd latest revision: %v", err)
		return err
	}
	lastRevision := resp.Header.Revision

	if ssr.prevSnapshot.Kind == snapstore.SnapshotKindFull && ssr.prevSnapshot.LastRevision == lastRevision {
		ssr.logger.Infof("There are no updates since last snapshot, skipping full snapshot.")
	} else {
		connectionCtx, cancel = context.WithTimeout(ssr.ctx, ssr.config.etcdConnectionTimeout*time.Second)
		defer cancel()
		rc, err := client.Snapshot(connectionCtx)
		if err != nil {
			ssr.logger.Errorf("failed to get etcd snapshot reader: %v", err)
			return err
		}
		ssr.logger.Infof("Successfully opened snapshot reader on etcd")
		s := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, lastRevision)
		startTime := time.Now()
		if err := ssr.config.store.Save(ssr.ctx, *s, rc); err != nil {
			timeTaken := time.Since(startTime).Seconds()
			metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
			ssr.logger.Errorf("failed to save snapshot: %v", err)
			return err
		}

		timeTaken := time.Since(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
		logrus.Infof("Total time to save snapshot: %f seconds.", timeTaken)
		ssr.prevSnapshot = s

		metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
		metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))

		ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	}

	if ssr.config.deltaSnapshotIntervalSeconds < 1 {
		// return without creating a watch on events
		return nil
	}

	watchCtx, cancelWatch := context.WithCancel(ssr.ctx)
	ssr.cancelWatch = cancelWatch
	ssr.etcdClient = client
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	return nil
}

func (ssr *Snapshotter) cleanupInMemoryEvents() {
	ssr.events = []byte{}
	ssr.lastEventRevision = -1
}

func (ssr *Snapshotter) takeDeltaSnapshotAndResetTimer() error {
	if err := ssr.TakeDeltaSnapshot(); err != nil {
		// As per design principle, in business critical service if backup is not working,
		// it's better to fail the process. So, we are quiting here.
		ssr.logger.Warnf("Taking delta snapshot failed: %v", err)
		return err
	}

	if ssr.deltaSnapshotTimer == nil {
		ssr.deltaSnapshotTimer = time.NewTimer(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	} else {
		ssr.logger.Infof("Stopping delta snapshot...")
		ssr.deltaSnapshotTimer.Stop()
		ssr.logger.Infof("Resetting delta snapshot to run after %d secs.", ssr.config.deltaSnapshotIntervalSeconds)
		ssr.deltaSnapshotTimer.Reset(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	}
	return nil
}

// TakeDeltaSnapshot takes a delta snapshot that contains
// the etcd events collected up till now
func (ssr *Snapshotter) TakeDeltaSnapshot() error {
	defer ssr.cleanupInMemoryEvents()
	ssr.logger.Infof("Taking delta snapshot for time: %s", time.Now().Local())

	if len(ssr.events) == 0 {
		ssr.logger.Infof("No events received to save snapshot. Skipping delta snapshot.")
		return nil
	}
	ssr.events = append(ssr.events, byte(']'))

	snap := snapstore.NewSnapshot(snapstore.SnapshotKindDelta, ssr.prevSnapshot.LastRevision+1, ssr.lastEventRevision)
	snap.SnapDir = ssr.prevSnapshot.SnapDir

	// compute hash
	hash := sha256.New()
	if _, err := hash.Write(ssr.events); err != nil {
		return fmt.Errorf("failed to compute hash of events: %v", err)
	}
	ssr.events = hash.Sum(ssr.events)
	startTime := time.Now()
	if err := ssr.config.store.Save(ssr.ctx, *snap, ioutil.NopCloser(bytes.NewReader(ssr.events))); err != nil {
		timeTaken := time.Since(startTime).Seconds()
		metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(timeTaken)
		ssr.logger.Errorf("Error saving delta snapshots. %v", err)
		return err
	}
	timeTaken := time.Since(startTime).Seconds()
	metrics.SnapshotDurationSeconds.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta, metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(timeTaken)
	logrus.Infof("Total time to save delta snapshot: %f seconds.", timeTaken)
	ssr.prevSnapshot = snap
	metrics.LatestSnapshotRevision.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.LastRevision))
	metrics.LatestSnapshotTimestamp.With(prometheus.Labels{metrics.LabelKind: ssr.prevSnapshot.Kind}).Set(float64(ssr.prevSnapshot.CreatedOn.Unix()))
	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return nil
}

// CollectEventsSincePrevSnapshot takes the first delta snapshot on etcd startup.
func (ssr *Snapshotter) CollectEventsSincePrevSnapshot() error {
	// close any previous watch and client.
	ssr.closeEtcdClient()

	client, err := etcdutil.GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		ssr.logger.Errorf("failed to create etcd client: %v", err)
		return err
	}

	ctxForRevision, cancel := context.WithTimeout(ssr.ctx, ssr.config.etcdConnectionTimeout*time.Second)
	resp, err := client.Get(ctxForRevision, "", clientv3.WithLastRev()...)
	cancel()
	if err != nil {
		ssr.logger.Errorf("failed to get etcd latest revision: %v", err)
		return err
	}
	lastEtcdRevision := resp.Header.Revision

	if ssr.prevSnapshot.LastRevision == lastEtcdRevision {
		ssr.logger.Infof("No new events since last snapshot. Skipping initial delta snapshot.")
		return nil
	}

	watchCtx, cancelWatch := context.WithCancel(ssr.ctx)
	ssr.cancelWatch = cancelWatch
	ssr.etcdClient = client
	ssr.watchCh = client.Watch(watchCtx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
	ssr.logger.Infof("Applied watch on etcd from revision: %d", ssr.prevSnapshot.LastRevision+1)

	for {
		select {
		case wr, ok := <-ssr.watchCh:
			if !ok {
				// As per etcd implementation watch channel closes only when the
				// context passed to Watch API is canceled. Hence wrapping error
				// here in context error rather than creating common error.
				ssr.logger.Errorf("watch channel closed. %v", wr)
				return fmt.Errorf("watch channel closed. %v", wr)
			}
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return err
			}

			lastWatchRevision := wr.Events[len(wr.Events)-1].Kv.ModRevision
			if lastWatchRevision >= lastEtcdRevision {
				return nil
			}
		case <-ssr.ctx.Done():
			ssr.cleanupInMemoryEvents()
			return ssr.ctx.Err()
		}
	}
}

func (ssr *Snapshotter) handleDeltaWatchEvents(wr clientv3.WatchResponse) error {
	if err := wr.Err(); err != nil {
		return err
	}
	// aggregate events
	for _, ev := range wr.Events {
		timedEvent := newEvent(ev)
		jsonByte, err := json.Marshal(timedEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal events to json: %v", err)
		}
		if len(ssr.events) == 0 {
			ssr.events = append(ssr.events, byte('['))
		} else {
			ssr.events = append(ssr.events, byte(','))
		}
		ssr.events = append(ssr.events, jsonByte...)
		ssr.lastEventRevision = ev.Kv.ModRevision
	}
	ssr.logger.Debugf("Added events till revision: %d", ssr.lastEventRevision)
	if len(ssr.events) >= ssr.config.deltaSnapshotMemoryLimit {
		ssr.logger.Infof("Delta events memory crossed the memory limit: %d Bytes", len(ssr.events))
		return ssr.takeDeltaSnapshotAndResetTimer()
	}
	return nil
}

func newEvent(e *clientv3.Event) *event {
	return &event{
		EtcdEvent: e,
		Time:      time.Now(),
	}
}

func (ssr *Snapshotter) snapshotEventHandler() error {
	for {
		select {
		case <-ssr.fullSnapshotCh:
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				return err
			}
		case <-ssr.fullSnapshotTimer.C:
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				return err
			}
		case <-ssr.deltaSnapshotTimer.C:
			if ssr.config.deltaSnapshotIntervalSeconds >= 1 {
				if err := ssr.takeDeltaSnapshotAndResetTimer(); err != nil {
					return err
				}
			}
		case wr, ok := <-ssr.watchCh:
			if !ok {
				// As per etcd implementation watch channel closes only when the
				// context passed to Watch API is canceled. Hence wrapping error
				// here in context error rather than creating common error.
				ssr.logger.Errorf("watch channel closed at snapshot event handler. %v", wr)
				return context.Canceled
			}
			if err := ssr.handleDeltaWatchEvents(wr); err != nil {
				return err
			}
		case <-ssr.ctx.Done():
			ssr.cleanupInMemoryEvents()
			return ssr.ctx.Err()
		}
	}
}

func (ssr *Snapshotter) resetFullSnapshotTimer() error {
	now := time.Now()
	effective := ssr.config.schedule.Next(now)
	if effective.IsZero() {
		ssr.logger.Info("There are no backups scheduled for the future. Stopping now.")
		return fmt.Errorf("error in full snapshot schedule")
	}
	duration := effective.Sub(now)
	if ssr.fullSnapshotTimer == nil {
		ssr.fullSnapshotTimer = time.NewTimer(duration)
	} else {
		ssr.logger.Infof("Stopping full snapshot...")
		ssr.fullSnapshotTimer.Stop()
		ssr.logger.Infof("Resetting full snapshot to run after %s", duration)
		ssr.fullSnapshotTimer.Reset(duration)
	}
	ssr.logger.Infof("Will take next full snapshot at time: %s", effective)

	return nil
}
