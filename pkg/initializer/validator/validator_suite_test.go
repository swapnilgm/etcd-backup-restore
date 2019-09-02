package validator_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
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
	snapshotterDurationSeconds = 15
)

var (
	testCtx      = context.Background()
	logger       = logrus.New().WithField("suite", "validator")
	etcd         *embed.Etcd
	err          error
	keyTo        int
	endpoints    = []string{etcdEndpoint}
	etcdRevision int64
)

// fileInfo holds file information such as file name and file path
type fileInfo struct {
	name string
	path string
}

func TestValidator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Validator Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	var (
		data []byte
	)

	err = os.RemoveAll(outputDir)
	Expect(err).ShouldNot(HaveOccurred())

	err = os.RemoveAll(etcdDir)
	Expect(err).ShouldNot(HaveOccurred())

	etcd, err = utils.StartEmbeddedEtcd(testCtx, etcdDir, logger)
	Expect(err).ShouldNot(HaveOccurred())
	defer func() {
		etcd.Server.Stop()
		etcd.Close()
	}()

	populatorTimeout := time.Duration(snapshotterDurationSeconds * time.Second)
	populatorCtx, cancelPopulator := context.WithTimeout(testCtx, populatorTimeout)
	resp := &utils.EtcdDataPopulationResponse{}
	go utils.PopulateEtcd(populatorCtx, logger, endpoints, 0, math.MaxInt64, resp)
	defer cancelPopulator()

	deltaSnapshotPeriod := 5
	snapshotterTimeout := populatorTimeout + time.Duration(deltaSnapshotPeriod+2)*time.Second
	ctx, cancel := context.WithTimeout(testCtx, snapshotterTimeout)
	defer cancel()
	err = runSnapshotter(ctx, logger.Logger, deltaSnapshotPeriod, endpoints)
	Expect(err).Should(Equal(context.DeadlineExceeded))
	keyTo = resp.KeyTo
	etcdRevision = resp.EndRevision

	err = os.Mkdir(path.Join(outputDir, "temp"), 0700)
	Expect(err).ShouldNot(HaveOccurred())

	return data
}, func(data []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	os.RemoveAll(outputDir)
})

// runSnapshotter creates a snapshotter object and runs it for a duration specified by 'snapshotterDurationSeconds'
func runSnapshotter(ctx context.Context, logger *logrus.Logger, deltaSnapshotPeriod int, endpoints []string) error {
	var (
		store                          snapstore.SnapStore
		certFile                       string
		keyFile                        string
		caFile                         string
		insecureTransport              bool
		insecureSkipVerify             bool
		maxBackups                     = 1
		deltaSnapshotMemoryLimit       = 10 * 1024 * 1024 //10Mib
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
		deltaSnapshotMemoryLimit,
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
		logger,
		snapshotterConfig,
	)

	return ssr.Run(true)
}

// copyFile copies the contents of the file at sourceFilePath into the file at destinationFilePath. If no file exists at destinationFilePath, a new file is created before copying
func copyFile(sourceFilePath, destinationFilePath string) error {
	data, err := ioutil.ReadFile(sourceFilePath)
	if err != nil {
		return fmt.Errorf("unable to read source file %s: %v", sourceFilePath, err)
	}

	err = ioutil.WriteFile(destinationFilePath, data, 0700)
	if err != nil {
		return fmt.Errorf("unable to create destination file %s: %v", destinationFilePath, err)
	}

	return nil
}
