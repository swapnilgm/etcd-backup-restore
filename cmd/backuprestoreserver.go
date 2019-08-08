
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
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/sirupsen/logrus"
	"github.com/gardener/etcd-backup-restore/pkg/config"
)

type backupRestoreServer struct {
	config *config.BackupRestoreComponentConfig
	logger *logrus.Entry
}

func newBackupRestoreServer(logger *logrus.Logger,config *config.BackupRestoreComponentConfig) *BackupRestoreServer{
   return &backupRestoreServer{
	   logger: logger.WithField("thread", "backup-restore-server"),
	   config: config,
   }
}

func(b *backupRestoreServer) run() error {
	var (
		snapstoreConfig    *snapstore.Config
		ssr                *snapshotter.Snapshotter
		handler            *server.HTTPHandler
		snapshotterEnabled bool
		err error
	)

	peerUrls, _ := types.NewURLs(b.config)
	peerUrls, _ := types.NewURLs(restorePeerURLs)

	options := &restorer.RestoreOptions{
		Ctx:                    ctx,
		RestoreDataDir:         path.Clean(restoreDataDir),
		Name:                   restoreName,
		ClusterURLs:            clusterUrlsMap,
		PeerURLs:               peerUrls,
		ClusterToken:           restoreClusterToken,
		SkipHashCheck:          skipHashCheck,
		MaxFetchers:            restoreMaxFetchers,
		EmbeddedEtcdQuotaBytes: embeddedEtcdQuotaBytes,
	}

	if storageProvider == "" {
		snapshotterEnabled = false
		logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
	} else {
		snapshotterEnabled = true
		snapstoreConfig = &snapstore.Config{
			Provider:                storageProvider,
			Container:               storageContainer,
			Prefix:                  path.Join(storagePrefix, backupFormatVersion),
			MaxParallelChunkUploads: maxParallelChunkUploads,
			TempDir:                 snapstoreTempDir,
		}
	}

	etcdInitializer := initializer.NewInitializer(options, snapstoreConfig, logger)

	tlsConfig := etcdutil.NewTLSConfig(
		certFile,
		keyFile,
		caFile,
		insecureTransport,
		insecureSkipVerify,
		etcdEndpoints,
		etcdUsername,
		etcdPassword)

	if snapshotterEnabled {
		ss, err := snapstore.GetSnapstore(ctx, snapstoreConfig)
		if err != nil {
			logger.Fatalf("Failed to create snapstore from configured storage provider: %v", err)
		}
		logger.Infof("Created snapstore from provider: %s", storageProvider)

		snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
			fullSnapshotSchedule,
			ss,
			maxBackups,
			deltaSnapshotIntervalSeconds,
			deltaSnapshotMemoryLimit,
			time.Duration(etcdConnectionTimeout),
			time.Duration(garbageCollectionPeriodSeconds),
			garbageCollectionPolicy,
			tlsConfig)
		if err != nil {
			logger.Fatalf("failed to create snapshotter config: %v", err)
		}

		logger.Infof("Creating snapshotter...")
		ssrCtx, stopSsr := context.WithCancel(ctx)
		ssr = snapshotter.NewSnapshotter(
			ssrCtx,
			logger,
			snapshotterConfig,
		)

		handler = startHTTPServer(etcdInitializer, ssr)
		defer handler.Stop()

		ssrStopCh = make(chan struct{})
		go handleSsrStopRequest(handler, ssr, ackCh, ssrStopCh, ctx.Done())
		go handleAckState(handler, ackCh)

		defragSchedule, _ := cron.ParseStandard(defragmentationSchedule)
		go etcdutil.DefragDataPeriodically(ctx, tlsConfig, defragSchedule, time.Duration(etcdConnectionTimeout)*time.Second, ssr.TriggerFullSnapshot, logrus.NewEntry(logger))

		runEtcdProbeLoopWithSnapshotter(tlsConfig, handler, ssr, ssrStopCh, ctx.Done(), ackCh)
		return
	}
	// If no storage provider is given, snapshotter will be nil, in which
	// case the status is set to OK as soon as etcd probe is successful
	handler = startHTTPServer(etcdInitializer, nil)
	defer handler.Stop()

	// start defragmentation without trigerring full snapshot
	// after each successful data defragmentation
	defragSchedule, _ := cron.ParseStandard(defragmentationSchedule)
	go etcdutil.DefragDataPeriodically(ctx, tlsConfig, defragSchedule, time.Duration(etcdConnectionTimeout)*time.Second, nil, logrus.NewEntry(logger))

	runEtcdProbeLoopWithoutSnapshotter(tlsConfig, handler, ctx.Done(), ackCh)

}