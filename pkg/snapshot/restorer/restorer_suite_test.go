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

package restorer_test

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	outputDir                  = "../../../test/output"
	etcdDir                    = outputDir + "/default.etcd"
	snapstoreDir               = outputDir + "/snapshotter.bkp"
	etcdEndpoint               = "http://localhost:2379"
	snapshotterDurationSeconds = 100
)

var (
	testCtx   = context.Background()
	logger    *logrus.Entry
	etcd      *embed.Etcd
	err       error
	keyTo     int
	endpoints = []string{etcdEndpoint}
)

func TestRestorer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Restorer Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	logger = logrus.New().WithField("suite", "restorer")
	logger.Logger.SetOutput(GinkgoWriter)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()

	deltaSnapshotPeriod := 5
	populatorTimeout := time.Duration(snapshotterDurationSeconds * time.Second)
	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, populatorTimeout)
	defer cancelPopulator()
	resp := &utils.EtcdDataPopulationResponse{}
	go utils.PopulateEtcd(populatorCtx, logger, endpoints, 0, math.MaxInt64, resp)

	snapshotterTimeout := populatorTimeout + time.Duration(deltaSnapshotPeriod+2)*time.Second
	ctx, cancel := context.WithTimeout(testCtx, snapshotterTimeout)
	defer cancel()
	err = runSnapshotter(ctx, logger, deltaSnapshotPeriod, endpoints)

	Expect(err).Should(Or(Equal(context.DeadlineExceeded), Equal(context.Canceled)))
	keyTo = resp.KeyTo
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	os.RemoveAll(outputDir)
})

// runSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func runSnapshotter(ctx context.Context, logger *logrus.Entry, deltaSnapshotPeriod int, endpoints []string) error {
	var (
		store                          snapstore.SnapStore
		certFile                       string
		keyFile                        string
		caFile                         string
		insecureTransport              bool
		insecureSkipVerify             bool
		maxBackups                     = 1
		etcdConnectionTimeout          = time.Duration(10)
		garbageCollectionPeriodSeconds = time.Duration(60)
		schedule                       = "0 0 1 1 *"
		garbageCollectionPolicy        = snapshotter.GarbageCollectionPolicyLimitBased
		etcdUsername                   string
		etcdPassword                   string
	)

	store, err = snapstore.GetSnapstore(ctx, &snapstore.Config{Container: snapstoreDir, Provider: "Local"})
	if err != nil {
		return err
	}

	tlsConfig := etcdutil.NewTLSConfig(
		certFile,
		keyFile,
		caFile,
		insecureTransport,
		insecureSkipVerify,
		endpoints,
		etcdUsername,
		etcdPassword,
	)

	snapshotterConfig, err := snapshotter.NewSnapshotterConfig(
		schedule,
		store,
		maxBackups,
		deltaSnapshotPeriod,
		snapshotter.DefaultDeltaSnapMemoryLimit,
		etcdConnectionTimeout,
		garbageCollectionPeriodSeconds,
		garbageCollectionPolicy,
		tlsConfig,
	)
	if err != nil {
		return err
	}

	ssr := snapshotter.NewSnapshotter(
		ctx,
		logger.Logger,
		snapshotterConfig,
	)

	return ssr.Run(true)
}
