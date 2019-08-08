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

package config

import (
	"fmt"

	"github.com/coreos/etcd/pkg/types"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
)

const (
	defaultServerPort               = 8080
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
)

// BackupRestoreComponentConfig holds the component configuration.
type BackupRestoreComponentConfig struct {
	EtcdConnectionConfig *EtcdConnectionConfig `json:"etcdConnectionConfig,omitempty"`
	ServerConfig         *ServerConfig         `json:"serverConfig,omitempty"`
	SnapshotterConfig    *SnapshotterConfig    `json:"snapshotterConfig,omitempty"`
	SnapstoreConfig      *SnapstoreConfig      `json:"snapstoreConfig,omitempty"`
	RestorationConfig    *RestorationConfig    `json:"restorationConfig,omitempty"`
}

// NewBackupRestoreComponentConfig returns the backup-restore componenet config.
func NewBackupRestoreComponentConfig() *BackupRestoreComponentConfig {
	return &BackupRestoreComponentConfig{
		EtcdConnectionConfig: NewEtcdConnectionConfig(),
		ServerConfig:         NewServerConfig(),
		SnapshotterConfig:    NewSnapshotterConfig(),
		SnapstoreConfig:      NewSnapstoreConfig(),
		RestorationConfig:    NewRestorationConfig(),
	}
}

// AddFlags adds the flags to flagset.
func (c *BackupRestoreComponentConfig) AddFlags(fs *flag.FlagSet) {
	c.EtcdConnectionConfig.AddFlags(fs)
	c.ServerConfig.AddFlags(fs)
	c.SnapshotterConfig.AddFlags(fs)
	c.SnapstoreConfig.AddFlags(fs)
	c.RestorationConfig.AddFlags(fs)
}

// Validate validates the config.
func (c *BackupRestoreComponentConfig) Validate() error {
	if err := c.EtcdConnectionConfig.Validate(); err != nil {
		return err
	}
	if err := c.ServerConfig.Validate(); err != nil {
		return err
	}
	if err := c.SnapshotterConfig.Validate(); err != nil {
		return err
	}
	if err := c.SnapstoreConfig.Validate(); err != nil {
		return err
	}
	if err := c.RestorationConfig.Validate(); err != nil {
		return err
	}
	return nil
}

// ServerConfig holds the server config.
type ServerConfig struct {
	Port            uint `json:"port"`
	EnableProfiling bool `json:"enableProfiling"`
}

// NewServerConfig returns the config for http server
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:            defaultServerPort,
		EnableProfiling: false,
	}
}

// AddFlags adds the flags to flagset.
func (c *ServerConfig) AddFlags(fs *flag.FlagSet) {
	fs.UintVarP(&c.Port, "server-port", "p", c.Port, "port on which server should listen")
	fs.BoolVar(&c.EnableProfiling, "enable-profiling", c.EnableProfiling, "enable profiling")
}

// Validate validates the config.
func (c *ServerConfig) Validate() error {
	return nil
}

// SnapstoreConfig holds the snapstore config.
type SnapstoreConfig struct {
	StorageProvider         string `json:"storageProvider"`
	StorageContainer        string `json:"storageContainer"`
	StoragePrefix           string `json:"storagePrefix"`
	MaxParallelChunkUploads uint   `json:"maxParallelChunkUploads"`
	SnapstoreTempDir        string `json:"snapstoreTempDir"`
}

// NewSnapstoreConfig returns the snapstore config.
func NewSnapstoreConfig() *SnapstoreConfig {
	return &SnapstoreConfig{
		MaxParallelChunkUploads: 5,
		SnapstoreTempDir:        "/tmp",
	}
}

// AddFlags adds the flags to flagset.
func (c *SnapstoreConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.StorageProvider, "storage-provider", c.StorageProvider, "snapshot storage provider")
	fs.StringVar(&c.StorageContainer, "store-container", c.StorageContainer, "container which will be used as snapstore")
	fs.StringVar(&c.StoragePrefix, "store-prefix", c.StoragePrefix, "prefix or directory inside container under which snapstore is created")
	fs.UintVar(&c.MaxParallelChunkUploads, "max-parallel-chunk-uploads", c.MaxParallelChunkUploads, "maximum number of parallel chunk uploads allowed ")
	fs.StringVar(&c.SnapstoreTempDir, "snapstore-temp-directory", c.SnapstoreTempDir, "temporary directory for processing")
}

// Validate validates the config.
func (c *SnapstoreConfig) Validate() error {
	if c.MaxParallelChunkUploads <= 0 {
		return fmt.Errorf("max parallel chunk uploads should be greater than zero")
	}
	return nil
}

// EtcdConnectionConfig holds the etcd connection config.
type EtcdConnectionConfig struct {
	Endpoints          []string `json:"endpoints"`
	Username           string   `json:"username"`
	Password           string   `json:"password"`
	ConnectionTimeout  uint     `json:"connectionTimeout"`
	InsecureTransport  bool     `json:"insecureTransport"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
	CertFile           string   `json:"certFile"`
	KeyFile            string   `json:"keyFile"`
	CaFile             string   `json:"caFile"`
}

// NewEtcdConnectionConfig returns etcd connection config.
func NewEtcdConnectionConfig() *EtcdConnectionConfig {
	return &EtcdConnectionConfig{
		Endpoints:          []string{"127.0.0.1:2379"},
		ConnectionTimeout:  30,
		InsecureTransport:  true,
		InsecureSkipVerify: false,
	}
}

// AddFlags adds the flags to flagset.
func (c *EtcdConnectionConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringSliceVarP(&c.Endpoints, "endpoints", "e", c.Endpoints, "comma separated list of etcd endpoints")
	fs.StringVar(&c.Username, "etcd-username", c.Username, "etcd server username, if one is required")
	fs.StringVar(&c.Password, "etcd-password", c.Password, "etcd server password, if one is required")
	fs.UintVar(&c.ConnectionTimeout, "etcd-connection-timeout", c.ConnectionTimeout, "etcd client connection timeout")
	fs.BoolVar(&c.InsecureTransport, "insecure-transport", c.InsecureTransport, "disable transport security for client connections")
	fs.BoolVar(&c.InsecureSkipVerify, "insecure-skip-tls-verify", c.InsecureTransport, "skip server certificate verification")
	fs.StringVar(&c.CertFile, "cert", c.CertFile, "identify secure client using this TLS certificate file")
	fs.StringVar(&c.KeyFile, "key", c.KeyFile, "identify secure client using this TLS key file")
	fs.StringVar(&c.CaFile, "cacert", c.CaFile, "verify certificates of TLS-enabled secure servers using this CA bundle")
}

// Validate validates the config.
func (c *EtcdConnectionConfig) Validate() error {
	if c.ConnectionTimeout <= 0 {
		return fmt.Errorf("connection timeout should be greater than zero")
	}
	return nil
}

// SnapshotterConfig holds the snapshotter config.
type SnapshotterConfig struct {
	FullSnapshotSchedule           string `json:"schedule"`
	DeltaSnapshotPeriodSeconds     uint   `json:"deltaSnapshotPeriodSeconds"`
	DeltaSnapshotMemoryLimit       uint   `json:"deltaSnapshotMemoryLimit"`
	GarbageCollectionPeriodSeconds uint   `json:"garbageCollectionPeriod_seconds"`
	GarbageCollectionPolicy        string `json:"garbageCollectionPolicy"`
	MaxBackups                     uint   `json:"maxBackups"`
	DefragmentationSchedule        string `json:"defragmentationSchedule"`
}

// NewSnapshotterConfig returns the snapshotter config.
func NewSnapshotterConfig() *SnapshotterConfig {
	return &SnapshotterConfig{
		FullSnapshotSchedule:           "* */1 * * *",
		DeltaSnapshotPeriodSeconds:     snapshotter.DefaultDeltaSnapshotIntervalSeconds,
		DeltaSnapshotMemoryLimit:       snapshotter.DefaultDeltaSnapMemoryLimit,
		GarbageCollectionPeriodSeconds: 60,
		GarbageCollectionPolicy:        snapshotter.GarbageCollectionPolicyExponential,
		MaxBackups:                     snapshotter.DefaultMaxBackups,
		DefragmentationSchedule:        "* * */3 * *",
	}
}

// AddFlags adds the flags to flagset.
func (c *SnapshotterConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVarP(&c.FullSnapshotSchedule, "schedule", "s", c.FullSnapshotSchedule, "schedule for snapshots")
	fs.UintVar(&c.DeltaSnapshotPeriodSeconds, "delta-snapshot-period-seconds", c.DeltaSnapshotPeriodSeconds, "Period in seconds after which delta snapshot will be persisted. If this value is set to be lesser than 1, delta snapshotting will be disabled.")
	fs.UintVar(&c.DeltaSnapshotMemoryLimit, "delta-snapshot-memory-limit", c.DeltaSnapshotMemoryLimit, "memory limit after which delta snapshots will be taken")
	fs.UintVar(&c.GarbageCollectionPeriodSeconds, "garbage-collection-period-seconds", c.GarbageCollectionPeriodSeconds, "Period in seconds for garbage collecting old backups")
	fs.StringVar(&c.GarbageCollectionPolicy, "garbage-collection-policy", c.GarbageCollectionPolicy, "Policy for garbage collecting old backups")
	fs.UintVarP(&c.MaxBackups, "max-backups", "m", c.MaxBackups, "maximum number of previous backups to keep")
	fs.StringVar(&c.DefragmentationSchedule, "defragmentation-schedule", c.DefragmentationSchedule, "schedule to defragment etcd data directory")
}

// Validate validates the config.
func (c *SnapshotterConfig) Validate() error {
	if _, err := cron.ParseStandard(c.FullSnapshotSchedule); err != nil {
		return err
	}
	if c.GarbageCollectionPolicy == snapshotter.GarbageCollectionPolicyLimitBased && c.MaxBackups <= 0 {
		return fmt.Errorf("max backups should be greather than zero for garbage collection policy set to limit based")
	}
	if _, err := cron.ParseStandard(c.DefragmentationSchedule); err != nil {
		return err
	}
	return nil
}

// ValidatorConfig holds the validation config.
type ValidatorConfig struct {
	ValidationMode    string `json:"validationMode"`
	FailBelowRevision uint64 `json:"experimentalFailBelowRevision"`
}

// NewValidatorConfig returns the validation config.
func NewValidatorConfig() *ValidatorConfig {
	return &ValidatorConfig{
		ValidationMode: string(validator.Full),
	}
}

// AddFlags adds the flags to flagset.
func (c *ValidatorConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ValidationMode, "validation-mode", string(c.ValidationMode), "mode to do data initialization[full/sanity]")
	fs.Uint64Var(&c.FailBelowRevision, "experimental-fail-below-revision", c.FailBelowRevision, "minimum required etcd revision, below which validation fails")
}

// Validate validates the config.
func (c *ValidatorConfig) Validate() error {
	return nil
}

// RestorationConfig holds the restoration configuration.
type RestorationConfig struct {
	InitialCluster           string   `json:"initialCluster"`
	InitialClusterToken      string   `json:"initialClusterToken"`
	RestoreDataDir           string   `json:"restoreDataDir"`
	InitialAdvertisePeerURLs []string `json:"initialAdvertisePeerURLs"`
	Name                     string   `json:"name"`
	SkipHashCheck            bool     `json:"skipHashCheck"`
	MaxFetchers              uint     `json:"maxFetchers"`
	EmbeddedEtcdQuotaBytes   int64    `json:"embeddedEtcdQuotaBytes"`
}

// NewRestorationConfig returns the restoration config.
func NewRestorationConfig() *RestorationConfig {
	return &RestorationConfig{
		InitialCluster:           initialClusterFromName(defaultName),
		InitialClusterToken:      "etcd-cluster",
		RestoreDataDir:           fmt.Sprintf("%s.etcd", defaultName),
		InitialAdvertisePeerURLs: []string{defaultInitialAdvertisePeerURLs},
		Name:                     defaultName,
		SkipHashCheck:            false,
		MaxFetchers:              6,
		EmbeddedEtcdQuotaBytes:   int64(8 * 1024 * 1024 * 1024),
	}
}

// AddFlags adds the flags to flagset.
func (c *RestorationConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InitialCluster, "initial-cluster", c.InitialCluster, "initial cluster configuration for restore bootstrap")
	fs.StringVar(&c.InitialClusterToken, "initial-cluster-token", c.InitialClusterToken, "initial cluster token for the etcd cluster during restore bootstrap")
	fs.StringVarP(&c.RestoreDataDir, "data-dir", "d", c.RestoreDataDir, "path to the data directory")
	fs.StringArrayVar(&c.InitialAdvertisePeerURLs, "initial-advertise-peer-urls", c.InitialAdvertisePeerURLs, "list of this member's peer URLs to advertise to the rest of the cluster")
	fs.StringVar(&c.Name, "name", c.Name, "human-readable name for this member")
	fs.BoolVar(&c.SkipHashCheck, "skip-hash-check", c.SkipHashCheck, "ignore snapshot integrity hash value (required if copied from data directory)")
	fs.UintVar(&c.MaxFetchers, "max-fetchers", c.MaxFetchers, "maximum number of threads that will fetch delta snapshots in parallel")
	fs.Int64Var(&c.EmbeddedEtcdQuotaBytes, "embedded-etcd-quota-bytes", c.EmbeddedEtcdQuotaBytes, "maximum backend quota for the embedded etcd used for applying delta snapshots")
}

// Validate validates the config.
func (c *RestorationConfig) Validate() error {
	if _, err := types.NewURLsMap(c.InitialCluster); err != nil {
		return fmt.Errorf("failed creating url map for restore cluster: %v", err)
	}
	if _, err := types.NewURLs(c.InitialAdvertisePeerURLs); err != nil {
		return fmt.Errorf("failed parsing peers urls for restore cluster: %v", err)
	}
	if c.MaxFetchers <= 0 {
		return fmt.Errorf("max fetchers should be greater than zero")
	}
	if c.EmbeddedEtcdQuotaBytes <= 0 {
		return fmt.Errorf("Etcd Quota size for etcd must be greater than 0")
	}
	return nil
}

// ApplyTo applies this config to other.
func (c *RestorationConfig) ApplyTo(other *RestorationConfig) error {
	if len(c.InitialAdvertisePeerURLs) != 0 {
		other.InitialAdvertisePeerURLs = c.InitialAdvertisePeerURLs
	}
	if len(c.InitialCluster) != 0 {
		other.InitialCluster = c.InitialCluster
	}
	return nil
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}
