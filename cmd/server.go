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

package cmd

import (
	"io"
	"context"
	"fmt"
	"net/http"
	"path"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/server"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	cron "github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
)

// NewServerCommand create cobra command for snapshot.
func NewServerCommand(ctx context.Context, out, errOut io.Writer) *cobra.Command {
	opts:= newOptions(out, errOut)
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "start the http server with backup scheduler.",
		Long:  `Server will keep listening for http request to deliver its functionality through http endpoints.`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersionInfo()
			if err:= opts.loadConfigFromFile(); err !=nil {
				opts.Logger.Fatlaf("failed to load the config from file: %v",err)
			return
		}
			if err:=opts.valEidate(); err !=nil {
				opts.Logger.Fatalf("failed to validate the options: %v",err)
				return
			}
			if err:=opts.run(); err !=nil {
				opts.Logger.Fatalf("failed to run with options: %v",err)
			}
		},
	}
	opts.AddFlags(serverCmd.Flags())
	return serverCmd
}

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func startHTTPServer(initializer initializer.Initializer, ssr *snapshotter.Snapshotter) *server.HTTPHandler {
	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &server.HTTPHandler{
		Port:            port,
		Initializer:     initializer,
		Snapshotter:     ssr,
		Logger:          logger,
		EnableProfiling: enableProfiling,
		ReqCh:           make(chan struct{}),
		AckCh:           make(chan struct{}),
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	logger.Info("Starting the http server...")
	go handler.Start()

	return handler
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is configured correctly
func runEtcdProbeLoopWithSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler, ssr *snapshotter.Snapshotter,  ackCh chan struct{}) error {
	var (
		err                       error
		initialDeltaSnapshotTaken bool
	)

	for {
		logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err = ProbeEtcd(ctx, tlsConfig)
		}
		if err != nil {
			logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}

		// The decision to either take an initial delta snapshot or
		// or a full snapshot directly is based on whether there has
		// been a previous full snapshot (if not, we assume the etcd
		// to be a fresh etcd) or it has been more than 24 hours since
		// the last full snapshot was taken.
		// If this is not the case, we take a delta snapshot by first
		// collecting all the delta events since the previous snapshot
		// and take a delta snapshot of these (there may be multiple
		// delta snapshots based on the amount of events collected and
		// the delta snapshot memory limit), after which a full snapshot
		// is taken and the regular snapshot schedule comes into effect.

		// TODO: write code to find out if prev full snapshot is older than it is
		// supposed to be, according to the given cron schedule, instead of the
		// hard-coded "24 hours" full snapshot interval
		if ssr.PrevFullSnapshot != nil && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= 24 {
			err := ssr.CollectEventsSincePrevSnapshot()
			if err {
				logger.Info("Snapshotter stopped.")
				ackCh <- emptyStruct
				handler.SetStatus(http.StatusServiceUnavailable)
				logger.Info("Shutting down...")
				return
			}
			if err == nil {
				if err := ssr.TakeDeltaSnapshot(); err != nil {
					logger.Warnf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
					continue
				}
				initialDeltaSnapshotTaken = true
			} else {
				logger.Warnf("Failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if !initialDeltaSnapshotTaken {
			if err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				logger.Errorf("Failed to take substitute first full snapshot: %v", err)
				continue
			}
		}

		// set server's healthz endpoint status to OK so that
		// etcd is marked as ready to serve traffic
		handler.SetStatus(http.StatusOK)

		ssr.SsrStateMutex.Lock()
		ssr.SsrState = snapshotter.SnapshotterActive
		ssr.SsrStateMutex.Unlock()

		go ssr.RunGarbageCollector()
		logger.Infof("Starting snapshotter...")
		if err := ssr.Run(initialDeltaSnapshotTaken); err != nil {
			if etcdErr, ok := err.(*errors.EtcdError); ok == true {
				logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
			} else {
				return err
			}
		}
		logger.Infof("Snapshotter stopped.")
		ackCh <- emptyStruct
		handler.SetStatus(http.StatusServiceUnavailable)
	}
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is not configured
func runEtcdProbeLoopWithoutSnapshotter(ctx context.Context, tlsConfig *etcdutil.TLSConfig, handler *server.HTTPHandler, ackCh chan struct{}) error {
	for {
		logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			handler.SetStatus(http.StatusServiceUnavailable)
			return ctx.Err()
		default:
			err = ProbeEtcd(ctx, tlsConfig)

			if err != nil {
				logger.Errorf("Failed to probe etcd: %v", err)
				switch err.(type) {
				case context.DeadlineExceeded, context.Canceled:
					return
				}
				handler.SetStatus(http.StatusServiceUnavailable)
				continue
			}
		}

		handler.SetStatus(http.StatusOK)
		<-ctx.Done()
		handler.SetStatus(http.StatusServiceUnavailable)
		return ctx.Err()
	}
}

// initializeServerFlags adds the flags to <cmd>
func initializeServerFlags(serverCmd *cobra.Command) {
	serverCmd.Flags().IntVarP(&port, "server-port", "p", defaultServerPort, "port on which server should listen")
	serverCmd.Flags().BoolVar(&enableProfiling, "enable-profiling", false, "enable profiling")
}

// ProbeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func ProbeEtcd(ctx context.Context, tlsConfig *etcdutil.TLSConfig) error {
	client, err := etcdutil.GetTLSClientForEtcd(tlsConfig)
	if err != nil {
		logger.Errorf("failed to create etcd client: %v", err)
		return err
	}

	connectionCtx, cancel := context.WithTimeout(ctx, time.Duration(etcdConnectionTimeout)*time.Second)
	defer cancel()
	if _, err := client.Get(connectionCtx, "foo"); err != nil {
		logger.Errorf("Failed to connect to client: %v", err)
		return err
	}
	return nil
}

// handleSsrStopRequest responds to handlers request and stop interrupt.
func handleSsrStopRequest(ctx context.Context, cancelSsr func(), handler *server.HTTPHandler, ackCh  chan struct{}) {
	for {
		var ok bool
		select {
		case _, ok = <-handler.ReqCh:
		case _, ok = <-ctx.Done():
		}

		ssr.SsrStateMutex.Lock()
		if ssr.SsrState == snapshotter.SnapshotterActive {
			ssr.SsrStateMutex.Unlock()
			cancelSsr()
		} else {
			ssr.SsrState = snapshotter.SnapshotterInactive
			ssr.SsrStateMutex.Unlock()
			ackCh <- emptyStruct
		}
		if !ok {
			return
		}
	}
}