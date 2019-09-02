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

package miscellaneous

import (
	"context"
	"sort"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

// GetLatestFullSnapshotAndDeltaSnapList returns the latest snapshot
func GetLatestFullSnapshotAndDeltaSnapList(ctx context.Context, store snapstore.SnapStore) (*snapstore.Snapshot, snapstore.SnapList, error) {
	var deltaSnapList snapstore.SnapList
	snapList, err := store.List(ctx)
	if err != nil {
		return nil, nil, err
	}

	for index := len(snapList); index > 0; index-- {
		if snapList[index-1].IsChunk {
			continue
		}
		if snapList[index-1].Kind == snapstore.SnapshotKindFull {
			sort.Sort(deltaSnapList)
			return snapList[index-1], deltaSnapList, nil
		}
		deltaSnapList = append(deltaSnapList, snapList[index-1])
	}
	return nil, deltaSnapList, nil
}
