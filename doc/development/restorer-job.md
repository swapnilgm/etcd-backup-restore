# Introducing new Restorer/Compactor Job

## Goal

Separate out the restoration logic from the backup-restore sidecar to new restorer job. Make it configurable in such a way that it would be also uses as compactor for the delta snapshots residing on object store.

## Motivation
Please refer issues
- https://github.com/gardener/etcd-backup-restore/issues/71
- https://github.com/gardener/etcd-backup-restore/issues/61
- https://github.com/gardener/etcd-backup-restore/issues/121

## Design

### Requirements
Currently, we deploy etcd as statefulset with backup-restore sidecar, where backup-restore sidecar solely handle the responsibility of scheduled backup (full + delta snapshots) and garbage collection of old snapshots and verification of data directory and restoration of data directory. Out-of-which scheduled backup and garbage collection are comparatively consuming low memory and time and doesn't have much affect on HA of etcd. For this two functionality, memory is completely independent of etcd DB content or for that matter db size. On other hand, verification process is takes time dependent on DB size. Though the memory consumption from verification process is currently dependent on data load on etcd, post merging of the PR https://github.com/etcd-io/etcd/pull/10516 it will constant memory independent of DB size. Restoration process requires fetching snapshots from object store which is surely DB size dependent. Also, restoration logic requires us to start an embedded etcd for which memory consumption is correlated with the DB size.

For large full snapshot interval, there will number of delta snapshot accumulated over time. And as a part of restoration this leads to additional time consumption since we have to apply delta snapshots sequentially. So the need of compactor is pretty clear to merge the delta snapshot into previous full snapshot, independent of existing backup schedule and without involving actual etcd anywhere.

### Approach for compaction
Basic approach for compaction is to restore etcd data directory at temporary location using base full snapshot and delta snapshot based on it using logic similar to existing restoration . And then take full snapshot out of this restored data directory to push it back to object store. This compactor can be scheduled cronjob separate from etcd statefulset.

### Component change

So, to avoid giving additional memory to backup sidecar where memory spike is only at the time of restoration which happens or to be precise supposed to happen very rarely, here we propose to make it as a separate Job out of statefulset and deploy it on need basis.

The container image in same job can be used for compaction as mentioned above with some configurable parameter, hence maintenance wise there will be only two binaries one for backup sidecar and one for restoration/compactor job.

The advantage of this approach is backup sidecar is solely responsible for backup and now the approach is more scalable for the multi-node cluster setup as well.
<!--In addition, the logic **can be** extended, to introduce etcd instance separate from actual instance as an init container in restoration job to make verify DB directory. This way we don't have to maintain explicit code for DB verification. **THINK OVER THIS AGAIN**-->

### Coordination

Now, the compactor, backup sidecar and restoration involves the object store update, hence to maintain objects store state always consistent with need all of them coordinate amongst them. There is need of some file based lock for this. We could use shared configmap for this purpose on which we can implement locking mechanism.

Now, the question remains to who will deploy the restoration job and when. Baking logic in backup sidecar is one approach but could possibly include too much complexity and also loose the modularity and scalability of code. As per the proposal https://github.com/gardener/etcd-backup-restore/pull/135 we have enough points to introduction of controller for managing etcd. Look into the health status of etcd and backup sidecar and decide to run restoration job. Since, PVC need to be shared between etcd statefulset and restoration job, someone need to scale down etcd pod and bring up restoration job when required. This responsibility can be also handled well by controller.

### Extension for multi-node etcd cluster
Regarding multi-node setup, we will have backup sidecar with each instance. We can have leader election on backup sidecar based on etcd leader, no need of addition resource like configmap or some DB for coordination. For restoration we will have configmap mentioned above associated with each etcd node of cluster.

### Roadmap
Bringing entire architecture change together will stop the stabilized development on existing architecture.
1. Backup sidecar with leader election based on etcd
    - Currently we have `etcdbrctl snapshot` command which start schedule snapshot of etcd endpoint provided to it.
    - In this step, we will simply add coordination/election logic among such multiple backup sidecar associated with multiple node is etcd cluster.
    - Though metrics are not exposed as a part of this command. We might have to add that here. or at the end of all steps below.
2. Verification + restoration job:
    - Currently verification job happens at every restart of etcd.
    - We already have `etcdbrctl initialize` which does the verification and if required restoration job.
    - Since both are sequential jobs its quite ok to pack them together.
    - we need an additional `configmap` field and need to embed logic for coordination among backup
    - ***In addition, we could use `terminationMessagePath` to optimize the decision of verification.
3. Compactor
    - restoration logic above can be used with different configurable parameter to work as compactor.
    - The coordinaiton configmap need to be updated accordingly.
4. Controller
    - Central controller will make task of coordination among restoration job and backup sidecar deployment.
    - Listen to different resource events and decide whether to deploy retoration job.
    - Do etcd version upgrade
    - Scale doen the backup sidecar/deployment.
