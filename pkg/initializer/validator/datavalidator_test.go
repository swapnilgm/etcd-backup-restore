package validator_test

import (
	"fmt"
	"os"
	"path"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/gardener/etcd-backup-restore/test/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
)

var _ = Describe("Running Datavalidator", func() {
	var (
		restoreDataDir     string
		snapstoreBackupDir string
		snapstoreConfig    *snapstore.Config
		validator          *DataValidator
	)

	BeforeEach(func() {
		restoreDataDir = path.Clean(etcdDir)
		snapstoreBackupDir = path.Clean(snapstoreDir)

		snapstoreConfig = &snapstore.Config{
			Container: snapstoreBackupDir,
			Provider:  "Local",
		}

		validator = &DataValidator{
			Config: &Config{
				DataDir:         restoreDataDir,
				SnapstoreConfig: snapstoreConfig,
			},
			Logger: logger.Logger,
		}
	})
	Context("with missing data directory", func() {
		It("should return DataDirStatus as DataDirectoryNotExist or DataDirectoryError, and non-nil error", func() {
			tempDir := fmt.Sprintf("%s.%s", restoreDataDir, "temp")
			err = os.Rename(restoreDataDir, tempDir)
			Expect(err).ShouldNot(HaveOccurred())
			dataDirStatus, err := validator.Validate(Full, 0)
			Expect(err).Should(HaveOccurred())
			Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryNotExist), Equal(DataDirectoryError)))
			err = os.Rename(tempDir, restoreDataDir)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("with data directory present", func() {
		Context("with incorrect data directory structure", func() {
			Context("with missing member directory", func() {
				It("should return DataDirStatus as DataDirectoryInvStruct or DataDirectoryError, and nil error", func() {
					memberDir := path.Join(restoreDataDir, "member")
					tempDir := fmt.Sprintf("%s.%s", memberDir, "temp")
					err = os.Rename(memberDir, tempDir)
					Expect(err).ShouldNot(HaveOccurred())
					dataDirStatus, err := validator.Validate(Full, 0)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryInvStruct), Equal(DataDirectoryError)))
					err = os.Rename(tempDir, memberDir)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
			Context("with member directory present", func() {
				Context("with missing snap directory", func() {
					It("should return DataDirStatus as DataDirectoryInvStruct or DataDirectoryError, and nil error", func() {
						snapDir := path.Join(restoreDataDir, "member", "snap")
						tempDir := fmt.Sprintf("%s.%s", snapDir, "temp")
						err = os.Rename(snapDir, tempDir)
						Expect(err).ShouldNot(HaveOccurred())
						dataDirStatus, err := validator.Validate(Full, 0)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryInvStruct), Equal(DataDirectoryError)))
						err = os.Rename(tempDir, snapDir)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})
				Context("with missing wal directory", func() {
					It("should return DataDirStatus as DataDirectoryInvStruct or DataDirectoryError, and nil error", func() {
						walDir := path.Join(restoreDataDir, "member", "wal")
						tempDir := fmt.Sprintf("%s.%s", walDir, "temp")
						err = os.Rename(walDir, tempDir)
						Expect(err).ShouldNot(HaveOccurred())
						dataDirStatus, err := validator.Validate(Full, 0)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryInvStruct), Equal(DataDirectoryError)))
						err = os.Rename(tempDir, walDir)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})

				Context("with empty wal directory and data validation in sanity mode", func() {
					It("should return DataDirStatus as DataDirectoryValid, and nil error", func() {
						walDir := path.Join(restoreDataDir, "member", "wal")
						tempWalDir := fmt.Sprintf("%s.%s", walDir, "temp")
						err = os.Rename(walDir, tempWalDir)
						Expect(err).ShouldNot(HaveOccurred())
						err = os.Mkdir(walDir, 0700)
						Expect(err).ShouldNot(HaveOccurred())
						dataDirStatus, err := validator.Validate(Sanity, 0)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
						err = os.RemoveAll(walDir)
						Expect(err).ShouldNot(HaveOccurred())
						err = os.Rename(tempWalDir, walDir)
						Expect(err).ShouldNot(HaveOccurred())
					})
				})
			})
		})
		Context("with correct data directory structure", func() {
			Context("with inconsistent revision numbers between etcd and latest snapshot", func() {
				It("should return DataDirStatus as RevisionConsistencyError or DataDirectoryError, and nil error", func() {
					tempDir := fmt.Sprintf("%s.%s", restoreDataDir, "temp")
					err = os.Rename(restoreDataDir, tempDir)
					Expect(err).ShouldNot(HaveOccurred())

					// start etcd
					etcd, err = utils.StartEmbeddedEtcd(testCtx, restoreDataDir, logger)
					Expect(err).ShouldNot(HaveOccurred())
					// populate etcd but with lesser data than previous populate call, so that the new db has a lower revision
					resp := &utils.EtcdDataPopulationResponse{}
					utils.PopulateEtcd(testCtx, logger, endpoints, 0, int(keyTo/2), resp)
					Expect(resp.Err).ShouldNot(HaveOccurred())
					etcd.Close()

					fmt.Printf("\nPrev etcd revision: %d\nNew etcd revision:  %d\n", etcdRevision, resp.EndRevision)

					// etcdRevision: latest revision number on the snapstore (etcd backup)
					// newEtcdRevision: current revision number on etcd db
					Expect(etcdRevision).To(BeNumerically(">=", resp.EndRevision))

					dataDirStatus, err := validator.Validate(Full, 0)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(RevisionConsistencyError), Equal(DataDirectoryError)))

					err = os.RemoveAll(restoreDataDir)
					Expect(err).ShouldNot(HaveOccurred())

					err = os.Rename(tempDir, restoreDataDir)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
			Context("with consistent revision numbers between etcd and latest snapshot", func() {
				Context("with corrupt data directory", func() {
					Context("with corrupt db file", func() {
						It("should return DataDirStatus as DataDirectoryCorrupt or DataDirectoryError or RevisionConsistencyError, and nil error", func() {
							dbFile := path.Join(restoreDataDir, "member", "snap", "db")
							_, err = os.Stat(dbFile)
							Expect(err).ShouldNot(HaveOccurred())

							tempFile := path.Join(outputDir, "temp", "db")
							err = copyFile(dbFile, tempFile)
							Expect(err).ShouldNot(HaveOccurred())

							file, err := os.OpenFile(
								dbFile,
								os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
								0666,
							)
							Expect(err).ShouldNot(HaveOccurred())
							defer file.Close()

							// corrupt the db file by writing random data to it
							byteSlice := []byte("Random data!\n")
							_, err = file.Write(byteSlice)
							Expect(err).ShouldNot(HaveOccurred())

							dataDirStatus, err := validator.Validate(Full, 0)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(int(dataDirStatus)).Should(SatisfyAny(Equal(DataDirectoryCorrupt), Equal(DataDirectoryError), Equal(RevisionConsistencyError)))

							err = os.Remove(dbFile)
							Expect(err).ShouldNot(HaveOccurred())

							err = os.Rename(tempFile, dbFile)
							Expect(err).ShouldNot(HaveOccurred())
						})
					})
				})
				Context("with clean data directory", func() {
					Context("with fail below revision configured to low value", func() {
						It("should return DataDirStatus as DataDirectoryValid, and nil error", func() {
							dataDirStatus, err := validator.Validate(Full, 0)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(int(dataDirStatus)).Should(Equal(DataDirectoryValid))
						})
					})

					Context("with fail below revision configured to high value", func() {
						It("should return DataDirStatus as FailBelowRevisionConsistencyError and nil error", func() {
							validator.Config.SnapstoreConfig.Container = path.Join(snapstoreBackupDir, "tmp")
							dataDirStatus, err := validator.Validate(Full, 1000000)
							Expect(err).ShouldNot(HaveOccurred())
							Expect(int(dataDirStatus)).Should(Equal(FailBelowRevisionConsistencyError))
						})
					})
				})
			})
		})
	})
})
