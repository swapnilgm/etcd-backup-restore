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

package etcdutil_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

const (
	outputDir       = "../../test/output"
	etcdDir         = outputDir + "/default.etcd"
	etcdEndpoint    = "http://localhost:2379"
	etcdDialTimeout = time.Second * 30
)

var (
	logger  = logrus.New().WithField("suite", "etcdutil")
	etcd    *embed.Etcd
	err     error
	testCtx = context.Background()
)

func TestEtcdutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Etcdutil Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	var data []byte
	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	etcd.Server.Stop()
	etcd.Close()
	os.RemoveAll(outputDir)
})
