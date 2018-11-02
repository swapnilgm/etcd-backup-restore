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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

// NewSnapshotter returns the snapshotter object.
func NewSnapshotter(config *Config, logger *logrus.Logger) *Snapshotter {
	// Create dummy previous snapshot
	prevSnapshot := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, 0)
	return &Snapshotter{
		logger:       logger,
		config:       config,
		prevSnapshot: prevSnapshot,
		wg:           sync.WaitGroup{},
		State:        SnapshotterStateInactive,
	}
}

// NewSnapshotterConfig returns a config for the snapshotter.
func NewSnapshotterConfig(schedule string, store snapstore.SnapStore, maxBackups, deltaSnapshotIntervalSeconds int, etcdConnectionTimeout, garbageCollectionPeriodSeconds time.Duration, garbageCollectionPolicy string, tlsConfig *TLSConfig) (*Config, error) {
	logrus.Info("Validating schedule...")
	sdl, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule provied %s : %v", schedule, err)
	}
	logrus.Infof("Schedule: %v", schedule)

	if garbageCollectionPolicy == GarbageCollectionPolicyLimitBased && maxBackups < 1 {
		return nil, fmt.Errorf("maximum backups limit should be greater than zero. Input MaxBackups: %d", maxBackups)
	}

	return &Config{
		schedule: sdl,
		store:    store,
		deltaSnapshotIntervalSeconds:   deltaSnapshotIntervalSeconds,
		etcdConnectionTimeout:          etcdConnectionTimeout,
		garbageCollectionPeriodSeconds: garbageCollectionPeriodSeconds,
		garbageCollectionPolicy:        garbageCollectionPolicy,
		maxBackups:                     maxBackups,
		tlsConfig:                      tlsConfig,
	}, nil
}

// NewTLSConfig returns the TLSConfig object.
func NewTLSConfig(cert, key, caCert string, insecureTr, skipVerify bool, endpoints []string) *TLSConfig {
	return &TLSConfig{
		cert:       cert,
		key:        key,
		caCert:     caCert,
		insecureTr: insecureTr,
		skipVerify: skipVerify,
		endpoints:  endpoints,
	}
}

// Run process loop for scheduled backup
func (ssr *Snapshotter) Run(skipInitialFullSnapshot bool, stopCh <-chan struct{}) error {
	defer ssr.stop()
	if !skipInitialFullSnapshot {
		if err := ssr.TakeFullSnapshot(); err != nil {
			return err
		}
	}

	var (
		now        = time.Now()
		effective  = ssr.config.schedule.Next(now)
		events     = []*event{}
		currentRev = ssr.prevSnapshot.LastRevision
	)

	if effective.IsZero() {
		ssr.logger.Info("There are no backup scheduled for future. Stopping now.")
		return nil
	}

	// Add event based on full snapshot period
	ssr.logger.Infof("Will take next full snapshot at time: %s", effective)
	ssr.fullSnapshotTimer = time.NewTimer(effective.Sub(now))
	if err := ssr.applyWatch(ssr.prevSnapshot.LastRevision + 1); err != nil {
		return err
	}

	for {
		select {
		case <-stopCh:
			return nil

		case _, ok := <-ssr.fullSnapshotTimer.C:
			if !ok {
				return nil
			}
			ssr.stop()
			ssr.logger.Infof("Taking scheduled snapshot for time: %s", time.Now().Local())
			if err := ssr.TakeFullSnapshot(); err != nil {
				// As per design principle, in business critical service if backup is not working,
				// it's better to fail the process. So, we are quiting here.
				return err
			}

			now = time.Now()
			effective = ssr.config.schedule.Next(now)
			if effective.IsZero() {
				ssr.logger.Info("There are no backup scheduled for future. Stopping now.")
				return nil
			}
			ssr.logger.Infof("Will take next full snapshot at time: %s", effective)
			ssr.fullSnapshotTimer.Reset(effective.Sub(now))
			if err := ssr.applyWatch(ssr.prevSnapshot.LastRevision + 1); err != nil {
				return err
			}

		case wr, ok := <-ssr.watchCh:
			if !ok {
				ssr.logger.Warn("watch channel closed")
				if err := ssr.applyWatch(currentRev + 1); err != nil {
					return err
				}
				break
			}

			if err := wr.Err(); err != nil {
				return fmt.Errorf("watch channel responded with err: %v", err)
			}

			//aggregate events
			for _, ev := range wr.Events {
				currentRev = ev.Kv.ModRevision
				events = append(events, newEvent(ev))
			}

		case <-ssr.deltaStopTimer.C:
			//persist aggregated events
			ssr.deltaStopTimer.Stop()
			if len(events) != 0 {
				if err := ssr.saveDeltaSnapshot(events, currentRev); err != nil {
					return fmt.Errorf("failed to take new delta snapshot: %s", err)
				}
				events = []*event{}
			} else {
				ssr.logger.Infof("No events received to save snapshot.")
			}
			ssr.deltaStopTimer.Reset(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
		}
	}
}

// stop stops the Snapshotter
func (ssr *Snapshotter) stop() {
	ssr.logger.Infof("Stop signal received. Terminating snapshotter...")
	if ssr.fullSnapshotTimer != nil {
		ssr.fullSnapshotTimer.Stop()
	}
	if ssr.deltaStopTimer != nil {
		ssr.deltaStopTimer.Stop()
	}
	ssr.cancelWatch()
}

// TakeFullSnapshot will store full snapshot of etcd to snapstore.
// It basically will connect to etcd. Then ask for snapshot. And finally
// store it to underlying snapstore on the fly.
func (ssr *Snapshotter) TakeFullSnapshot() error {
	client, err := GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), ssr.config.etcdConnectionTimeout*time.Second)
	defer cancel()
	// Note: Although Get and snapshot call are not atomic, so revision number in snapshot file
	// may be ahead of the revision found from GET call. But currently this is the only workaround available
	// Refer: https://github.com/coreos/etcd/issues/9037
	resp, err := client.Get(ctx, "", clientv3.WithLastRev()...)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to get etcd latest revision: %v", err),
		}
	}
	lastRevision := resp.Header.Revision
	ctx, cancel = context.WithTimeout(context.TODO(), ssr.config.etcdConnectionTimeout*time.Second)
	defer cancel()
	rc, err := client.Snapshot(ctx)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd snapshot: %v", err),
		}
	}
	ssr.logger.Infof("Successfully opened snapshot reader on etcd")
	s := snapstore.NewSnapshot(snapstore.SnapshotKindFull, 0, lastRevision)
	startTime := time.Now()
	if err := ssr.config.store.Save(*s, rc); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}
	logrus.Infof("Total time to save snapshot: %f seconds.", time.Now().Sub(startTime).Seconds())
	ssr.prevSnapshot = s
	ssr.logger.Infof("Successfully saved full snapshot at: %s", path.Join(s.SnapDir, s.SnapName))
	return nil
}

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *TLSConfig) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if tlsConfig.cert != "" {
		tlsinfo.CertFile = tlsConfig.cert
		cfgtls = &tlsinfo
	}

	if tlsConfig.key != "" {
		tlsinfo.KeyFile = tlsConfig.key
		cfgtls = &tlsinfo
	}

	if tlsConfig.caCert != "" {
		tlsinfo.CAFile = tlsConfig.caCert
		cfgtls = &tlsinfo
	}

	cfg := &clientv3.Config{
		Endpoints: tlsConfig.endpoints,
	}

	if cfgtls != nil {
		clientTLS, err := cfgtls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = clientTLS
	}

	// if key/cert is not given but user wants secure connection, we
	// should still setup an empty tls configuration for gRPC to setup
	// secure connection.
	if cfg.TLS == nil && !tlsConfig.insecureTr {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.skipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	return clientv3.New(*cfg)
}

// applyWatch applies the new watch on etcd
func (ssr *Snapshotter) applyWatch(revision int64) error {
	client, err := GetTLSClientForEtcd(ssr.config.tlsConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}
	client.Close()
	ctx := context.TODO()
	ctx, ssr.cancelWatch = context.WithCancel(ctx)
	ssr.watchCh = client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(revision))
	ssr.deltaStopTimer = time.NewTimer(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	return nil
}

// processWatch processess watch to take delta snapshot periodically by collecting set of events within period
func (ssr *Snapshotter) processWatch(client *clientv3.Client) {
	var (
		currentRev  int64
		emptyStruct struct{}
		ctx         = context.TODO()
		watchCh     = client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(ssr.prevSnapshot.LastRevision+1))
		events      = []*event{}
		timer       = time.NewTimer(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
	)
	ssr.logger.Infof("Applied watch on etcd from revision: %08d", ssr.prevSnapshot.LastRevision+1)
	defer ssr.wg.Done()
	defer client.Close()
	for {
		select {
		case <-ssr.deltaStopCh:
			ssr.logger.Info("Received stop signal. Terminating watch and Saving aggregated events if any...")
			//persist aggregated events
			if len(events) != 0 {
				if err := ssr.saveDeltaSnapshot(events, currentRev); err != nil {
					ssr.logger.Errorf("failed to take new delta snapshot: %s", err)
					return
				}
				events = []*event{}
			} else {
				ssr.logger.Infof("No events received to save snapshot.")
			}
			timer.Stop()
			return

		case wr, ok := <-watchCh:
			if !ok {
				ssr.logger.Warn("watch channel closed")
				ssr.fullSnapshotCh <- emptyStruct
				return
			}
			if err := wr.Err(); err != nil {
				ssr.logger.Warn("watch channel responded with err: %v", err)
				ssr.fullSnapshotCh <- emptyStruct
				return
			}

			//aggregate events
			for _, ev := range wr.Events {
				currentRev = ev.Kv.ModRevision
				events = append(events, newEvent(ev))
			}

		case <-timer.C:
			//persist aggregated events
			if len(events) != 0 {
				if err := ssr.saveDeltaSnapshot(events, currentRev); err != nil {
					ssr.logger.Errorf("failed to take new delta snapshot: %s", err)
					ssr.fullSnapshotCh <- emptyStruct
					timer.Stop()
					return
				}
				events = []*event{}
			} else {
				ssr.logger.Infof("No events received to save snapshot.")
			}
			timer.Reset(time.Second * time.Duration(ssr.config.deltaSnapshotIntervalSeconds))
		}
	}
}

// saveDeltaSnapshot save the delta events compared to previous snapshot in new snapshot of kind Incr.
func (ssr *Snapshotter) saveDeltaSnapshot(ops []*event, lastRevision int64) error {
	snap := &snapstore.Snapshot{
		Kind:          snapstore.SnapshotKindDelta,
		CreatedOn:     time.Now().UTC(),
		StartRevision: ssr.prevSnapshot.LastRevision + 1,
		LastRevision:  lastRevision,
		SnapDir:       ssr.prevSnapshot.SnapDir,
	}
	snap.GenerateSnapshotName()

	data, err := json.Marshal(ops)
	if err != nil {
		return err
	}

	// compute hash
	hash := sha256.New()
	hash.Write(data)
	data = hash.Sum(data)

	dataReader := bytes.NewReader(data)

	if err := ssr.config.store.Save(*snap, dataReader); err != nil {
		return &errors.SnapstoreError{
			Message: fmt.Sprintf("failed to save snapshot: %v", err),
		}
	}

	ssr.prevSnapshot = snap
	ssr.logger.Infof("Successfully saved delta snapshot at: %s", path.Join(snap.SnapDir, snap.SnapName))
	return nil
}

func newEvent(e *clientv3.Event) *event {
	return &event{
		EtcdEvent: e,
		Time:      time.Now(),
	}
}
