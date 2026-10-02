# daos-csi

A CSI driver (`csi.daos.io`) for DAOS POSIX containers, mounted with dfuse. It targets clusters
where pods run with user namespaces (`hostUsers: false`), under gVisor, or under the `restricted`
Pod Security level, and where applications use the libpil4dfs interception library.

Pods with `hostUsers: false` need a dfuse that allows idmapped mounts. There are two variants:
- **The upstream patch** `dfuse --allow-idmap` (proposed to DAOS; a dfuse option plus a fused
  update with `FUSE_CAP_ALLOW_IDMAP`), on DAOS master or ported to 2.8.0. Set
  `dfuseArgs: "--allow-idmap"` on the StorageClass.
- **An earlier, broader version** of the patch for DAOS 2.8 (not proposed upstream). It always
  allows idmapped mounts, and its libpil4dfs connects by itself when dfuse refuses its handles.

With stock DAOS everything else works.

## How it works

```
               controller (Deployment)                     node (DaemonSet, privileged)
  PVC ──► csi-provisioner ──► CreateVolume                kubelet ──► NodeStageVolume
                              daos container create                    └► mount pod: dfuse --foreground
                              <pool> <pvc-name> --type=POSIX              on .../globalmount (one per
                                                                          volume and node)
                                                          kubelet ──► NodePublishVolume
                                                                       └► bind mount into the pod
                                                          runc ──► idmaps the bind mount for
                                                                   hostUsers: false pods
```

- **Volumes** are DAOS POSIX containers identified as `<pool>/<container>`. Dynamic volumes get a
  new container named after the PV. Static PVs name an existing one in `volumeHandle`.
- **Mount pods** (`-mount-mode=pod`, the default). Each volume used on a node gets one dfuse
  process, running in its own pod in the `daos-csi` namespace. The node plugin can be restarted or
  upgraded without breaking running pods' volumes. `-mount-mode=process` runs dfuse inside the
  plugin container instead. That mode is simpler, but a plugin restart leaves running pods with
  `Transport endpoint is not connected`; the plugin remounts the staging path, but those pods
  have to be restarted.
- **Idmapped mounts.** With `--allow-idmap` (or the older always-on build), dfuse lets the container
  runtime idmap the bind mount for pods with `hostUsers: false`. This needs Linux 6.12 or later
  (Talos 1.9+, Ubuntu 24.04 HWE). Without it, or on older kernels, those pods fail with `failed to
  set MOUNT_ATTR_IDMAP ... invalid argument`.
- **Mount pods run with `hostPID: true`.** With `--allow-idmap`, dfuse only shares its pool and
  container handles with interception libraries in its pid namespace or one nested in it. In the
  host's pid namespace, that covers every pod.
- **The `agent-socket` inline volume** exposes the node's `daos_agent.sock` to pods. libpil4dfs and
  the daos CLI need it. Unlike a hostPath volume, it is allowed by the `baseline` and
  `restricted` Pod Security levels.
- **Staging lives under `/var/lib/kubelet`**, so no extra kubelet mounts are needed (on Talos, for
  example).

## Parameters

StorageClass `parameters` (dynamic) or PV `csi.volumeAttributes` (static):

| key | meaning |
|---|---|
| `pool` | DAOS pool (label or UUID). Required for dynamic volumes |
| `oclass`, `dirOclass`, `fileOclass`, `chunkSize`, `properties` | passed to `daos container create` |
| `group` | owner group of the container (`group@`) |
| `acl` | extra container ACL entries, comma separated, e.g. `A::admin@:rwta` |
| `rootUid`, `rootGid`, `rootMode` | owner/mode applied to the container root when it is first mounted, so non-root pods can write without `fsGroup` |
| `dfuseArgs` | extra dfuse arguments, e.g. `--allow-idmap` for pods with `hostUsers: false`, `--disable-caching` for data written from several nodes, `--thread-count=N` |
| `dfuseCpuRequest`, `dfuseCpuLimit`, `dfuseMemoryRequest`, `dfuseMemoryLimit` | resources of this volume's dfuse in its mount pods, overriding the node plugin's defaults (see "dfuse resources") |

## Who a pod is to DAOS

DAOS ACLs apply to the credential that `daos_agent` gives each process. The agent derives it from
the process's uid **as seen on the node**:

- **dfuse** in the mount pods runs as root and reaches DAOS as `root@`. The controller also creates
  containers as `root@`, so root is `OWNER@` of dynamic volumes. For static volumes, give `root@`
  (or the owner you use) an entry in the pool and container ACLs.
- **Pods using the volume through dfuse** do not need DAOS permissions of their own. dfuse
  (`--multi-user`, `default_permissions`) enforces POSIX modes against the pod's uid.
- **Pods using libpil4dfs** with dfuse's handles reach DAOS as dfuse does.
- **Interception libraries that connect by themselves** (libioil after a refusal, the first
  version's libpil4dfs, gVisor) need DAOS permissions of their own. With `hostUsers: false` their
  node-side uid is random and has no passwd entry, so the agent needs
  `credential_config.client_user_map.default`, and that principal needs an ACL entry (`acl`
  parameter). All such pods then look the same to DAOS.

## libpil4dfs in pods

dfuse shares its pool and container handles only with processes of its own uid and gid (root in
the mount pods). With `--allow-idmap`, it also requires them to be in its pid namespace or one
nested in it.

| Pod | Upstream patch (`--allow-idmap`) | First version (`patches/`) |
|---|---|---|
| `hostUsers: true`, root (the uid of dfuse) | libpil4dfs and libioil import dfuse's handles (ioctl) | the same |
| `hostUsers: false`, or another uid | dfuse refuses its handles. libpil4dfs does not intercept (`op_sum 0`), and I/O goes through dfuse. libioil connects by itself with the pod's credential | libpil4dfs asks dfuse for the pool and container UUIDs (allowed for everyone) and connects with the pod's own credential |
| gVisor (`runtimeClassName: gvisor`) | the volume shows up as 9p in the sandbox. libpil4dfs needs `D_IL_MOUNT_POINT`, `D_IL_POOL` and `D_IL_CONTAINER`, and then connects by itself | the same |

To see why libpil4dfs did not intercept, run with
`D_LOG_FILE=/tmp/il.log D_LOG_MASK=DEBUG DD_MASK=all`, then
`grep retrieve_handles_from_fuse /tmp/il.log`. "Operation not permitted" means that dfuse refused
its handles.

Requirements:
- The pod mounts the `agent-socket` inline volume at `/var/run/daos_agent`.
- Traffic from pods to the DAOS servers must not be NATed. DAOS servers talk back to the client's
  pod IP, so the servers need a route for the pod CIDR, and the CNI must not masquerade traffic
  to the storage network.
  - With Cilium, use `ipMasqAgent` / `nonMasqueradeCIDRs`.
  - With Calico, add an IPPool for the storage network with `disabled: true`.
  - With flannel (Talos' default) and other iptables CNIs, deploy `deploy/daos-pod-nonat.yaml`.
    `--ip-masq=false` alone does not help: flannel's CNI plugin then turns on per-pod masquerading
    in the bridge plugin.

  Symptoms of NAT: `DER_TIMEDOUT` in the pod, and `NA_TIMEOUT ... from ofi+tcp://<pod ip>` in the
  engine log.
- For gVisor, runsc needs `host-uds = "open"`. On Talos this is set in the machine config (see
  below).

See `examples/pil4dfs-restricted.yaml` and `examples/gvisor.yaml`.

## Deploy

1. Run `daos_agent` on every node, either as a host service or as `deploy/daos-agent.yaml`. Set
   `disable_auto_evict: true`: otherwise restarting the agent evicts dfuse's pool handles and
   every mount on the node breaks.

   In a container, the agent turns uids into ACL principals using the container's
   `/etc/passwd`, not the node's. `client_user_map` only applies to uids that are *unknown* there.
   The DaemonSet therefore replaces `/etc/passwd` and `/etc/group` from its ConfigMap. Without
   that, for example, uid 1000 would become `ubuntu@` in an Ubuntu-based image.
2. Build the image from `Containerfile` on top of an image with the patched DAOS client (`BASE`),
   e.g. one built from a client tarball with `Containerfile.client-ubuntu`. Docker and Podman both
   work (checked with Docker 24 and 27). Docker needs `-f Containerfile`, as it only looks for a
   `Dockerfile` by default.
3. Set `DAOS_CSI_IMAGE` in `deploy/daos-csi.yaml`, set the StorageClass `pool`, then apply.

### With Helm

The chart in `helm/daos-csi` deploys the same objects as `deploy/`. The agent DaemonSet, the
no-NAT DaemonSet and StorageClasses are optional, and so is a ServiceMonitor for the Prometheus
Operator. Its `values.yaml` describes every setting.

The namespace must allow privileged pods: the node plugin creates its mount pods there.

```
kubectl create namespace daos-csi
kubectl label namespace daos-csi pod-security.kubernetes.io/enforce=privileged
helm install daos-csi helm/daos-csi -n daos-csi -f my-values.yaml
```

A minimal `my-values.yaml`, with daos_agent as a DaemonSet:

```yaml
image: {repository: registry.example.com/daos-csi, tag: "0.1.0"}
agent:
  enabled: true
  config:
    access_points: ['10.0.0.1']
    include_fabric_ifaces: [eth0]
storageClasses:
- name: daos
  parameters: {pool: tank, dfuseArgs: "--allow-idmap"}
```

`agent.config` is merged with the defaults of `values.yaml` (`disable_auto_evict`,
`client_user_map`, and so on). To drop a default key, set it to `null`.

The controller uses the host network and its metrics port, so its rolling update stops the old
pod before starting the new one (`maxSurge: 0`).

## dfuse resources

Each volume used on a node has one dfuse, in its mount pod, however many pods use the volume
there. The settings below apply to mount pods (`-mount-mode=pod`, the default). With libpil4dfs or libioil, I/O runs in the application's own process and is accounted to
its container. Idle dfuse threads wait without polling.

The resources of dfuse are set as follows:
- **Defaults** come from the node plugin's flags `-mount-pod-cpu-request`, `-mount-pod-cpu-limit`,
  `-mount-pod-memory-request` and `-mount-pod-memory-limit` (Helm: `node.mountPodResources`).
- **A volume** can override each of them with the StorageClass parameters `dfuseCpuRequest`,
  `dfuseCpuLimit`, `dfuseMemoryRequest` and `dfuseMemoryLimit` (or PV `volumeAttributes`).
- **When they apply.** They are set when the mount pod is created, i.e. when the volume is first
  used on the node. A running mount pod keeps its resources. Restarting it would break the pods
  using the volume.
- **Validation.** The controller checks the syntax and that no request exceeds its limit, so a
  bad StorageClass fails at provisioning.

Things to know:
- **A limit alone reserves nothing.** The driver then sets the request to 0, where Kubernetes would
  default it to the limit. Mount pods are created with `nodeName` set, so the scheduler does not
  know about them. kubelet admits them on its own, and as they are `system-node-critical`, it
  *evicts lower-priority pods* to make room for their requests ("Preempted in order to admit
  critical pod"). A limit of 2 CPUs on 50 volumes must not reserve 100 CPUs. Set a request only
  to reserve on purpose, and keep it small.
- **Requests that cannot fit** fail the mount: kubelet rejects the mount pod
  (`UnexpectedAdmissionError ... preemption`, or `OutOfcpu`). The driver deletes the rejected
  pod, reports the reason in the pod's `FailedMount` event, and tries again with backoff.
- **A CPU limit** caps dfuse with CFS throttling. In the lab, 200m brought one writer from
  135–180 to 40–55 MB/s over several runs.
- **Thread count.** dfuse sizes its thread pool from the CPUs it may run on (up to 16, or one per
  CPU of its cpuset), not from a CPU limit. More threads than the limit allows only get throttled.
  So with a CPU limit, the driver adds `--thread-count` = the limit rounded up plus the event
  queues (`--eq-count`, 1 by default): a limit of 2 CPUs gives 3 threads. A `--thread-count` in
  `dfuseArgs` is kept as is.
- **A memory limit** kills dfuse when it is exceeded. Its mount pod restarts dfuse, but pods that
  were using the volume keep a dead mount until they are restarted. Leave headroom.
- **Pinning.** With kubelet's static CPU manager, requests equal to limits in whole CPUs give
  dfuse cores of its own, and it then sizes its threads to them.

### Volumes per node

`-max-volumes-per-node=N` (Helm: `node.maxVolumesPerNode`) is reported to Kubernetes as the
node's volume limit for this driver. The scheduler then places pods on a node only while the
distinct DAOS volumes used there stay within N. That is the number of dfuse processes, so N times
the per-volume CPU and memory limits bounds what dfuse can use on the node. 0 (the default) means
no limit.

## Health checks

The node plugin probes every volume staged on its node, by default once a minute
(`-health-interval`, 0 to disable). The probe is a `statfs` of the staging path. dfuse answers it
with a DAOS pool query, a round trip to the servers that the kernel never caches, so the probe
fails when dfuse or its network path is stuck, e.g. on a dead network port. Probes run in the
background and never block CSI calls, even on a hung mount.

- **Unhealthy.** A volume whose probe fails, goes unanswered for `-health-timeout` (30s), or whose
  staging path is not mounted gets `daos_csi_volume_healthy{volume}` 0 and a log line.
- **Kill and restart.** After `-health-kill-after` (5m) of that, the dfuse of the volume is killed.
  Its mount pod starts a new one (in `-mount-mode=process`, the plugin does).
  - If the servers are still unreachable, the new dfuse mounts once they are back.
  - Every new dfuse gets the whole `-health-kill-after` from when its mount appears. Right after
    an outage it may still be connecting, and answers of probes of the dfuse it replaced do not
    count.
  - The kill is the only disruptive step. The default is long enough that a slow but working
    DAOS system does not lose its mounts. 0 turns it off.
- **Pods' mounts.** A running container keeps the mount of the dfuse it started with, and that
  mount is dead now. The plugin bind-mounts the new dfuse on every pod's mount point of the
  volume on the node (`daos_csi_targets_rebound_total`), and the container's next start uses it.
  So an application recovers by itself if its liveness probe covers the volume, e.g.:

  ```yaml
  livenessProbe:
    exec: {command: [stat, -f, /data]}   # the volume's mount path
    periodSeconds: 10
    timeoutSeconds: 5
    failureThreshold: 3
  ```

  Without such a probe, restart the pod.
- **Lab results.** Both a frozen dfuse (`SIGSTOP`) and a node cut off from the DAOS servers
  (iptables) were detected. dfuse was replaced, and a pod with the probe above read its data again
  without being recreated (lab suite `k8s-csi-health`).
- **Space.** `NodeGetVolumeStats` answers from the last successful probe, without touching the
  mount, so kubelet's `kubelet_volume_stats_*` metrics show the pool's space. kubelet must have
  `volumeStatsAggPeriod` above 0; kubeadm writes 0s, which turns them off. CSI 1.13 dropped the
  volume condition, and its successor is alpha, so health is reported through the metrics above.
- **Cost.** Each probe is a pool query with space information, which the pool service gathers
  from the pool's engines. With many nodes and volumes, raise the interval: 1000 nodes with 10
  volumes each and the default interval make about 170 pool queries per second.

## Monitoring and logs

`-http-endpoint=<host:port>` serves three paths. The manifests use port 9810 on the node plugin
and 9811 on the controller, both on the host network, with `prometheus.io/*` annotations.
- `/healthz` and `/readyz` are for the liveness and readiness probes.
- `/metrics` is in the Prometheus text format:

| Metric | Meaning |
|---|---|
| `daos_csi_operations_total{method,code}` | CSI calls by gRPC result code |
| `daos_csi_operation_duration_seconds{method}` | Call duration (histogram) |
| `daos_csi_dfuse_mounts_total{result}` | dfuse mounts of a staging path, `ok` or `error` |
| `daos_csi_dfuse_mount_duration_seconds` | Time until a started dfuse serves its staging path |
| `daos_csi_stale_mounts_total{where}` | Mounts whose dfuse had died, found and cleared: `staging`, or `target` (a pod's mount, replaced when the pod is published again) |
| `daos_csi_staged_volumes` | dfuse mounts at this node's staging paths (node plugin), read from mountinfo |
| `daos_csi_volume_healthy{volume}` | 1 if the volume's dfuse answered its last health probe in time |
| `daos_csi_health_probe_failures_total{reason}` | failed probes: `timeout`, `error`, `dead` (dfuse gone) |
| `daos_csi_dfuse_kills_total` | unresponsive dfuse processes killed |
| `daos_csi_targets_rebound_total` | pods' mounts switched to a volume's new dfuse |

`daos_csi_staged_volumes` deliberately does not stat the mounts, because a stat of a hung FUSE mount
blocks. Hung mounts are found by the health checks.

Logging options:
- `-log-level=debug|info|warn|error`. At `debug` the full request of every call is logged.
- `-log-format=text|json`.

Each call is logged with its method, volume, target path and duration.

## Development

```
go vet ./... && go test -race ./...
```

The unit tests replace the mount layer (`mounter`) with an in-memory fake. Among them:
- concurrent publishes and unpublishes of one volume by several pods: each call must act on its
  own target;
- replacing a stale pod mount;
- mount pod resources and the thread count derived from a CPU limit;
- health checks on a fake node (staging area, pods' `vol_data.json`, mountinfo, `/proc`):
  - a hung dfuse is found and killed, but not before `-health-kill-after`, and not when that is 0;
  - a new dfuse after an outage gets the whole `-health-kill-after`;
  - a dfuse that is gone is left to its mount pod;
  - pods' mounts of an old dfuse are mounted again;
- mountinfo parsing, the mount pod spec, and the metrics format.

Mounting itself is tested in a lab cluster (kubeadm and Talos) with Kubernetes test suites.

## Talos

The driver, the agent DaemonSet and `daos-pod-nonat` run on Talos unchanged: staging lives under
`/var/lib/kubelet`, so no `kubelet.extraMounts` are needed. Tested with Talos v1.14.1 (Linux 6.18,
containerd 2.3) and the `siderolabs/gvisor` extension. Machine config used in the lab:

- `user.max_user_namespaces` sysctl set to a non-zero value (`hostUsers: false`)
- gVisor host UDS: a runsc config under `/var` plus a CRI customization pointing the `runsc`
  runtime at it (`machine.files`, `/etc/cri/conf.d/20-customization.part`):
  ```toml
  [plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/etc/runsc/runsc.toml"   # contains: [runsc_config] host-uds = "open"
  ```
- `daos-pod-nonat` (or a CNI configured not to masquerade the storage network) for libpil4dfs.
  The DAOS servers need a route for the pod CIDR.

## Tested

The same scenario suite (`hack/run-scenarios.sh`) ran on:
- **kubeadm:** 1 node, Linux 7.2, Kubernetes 1.37, containerd 2.4, runc 1.5, gVisor 20260921.
- **Talos:** v1.14.1, Linux 6.18, Kubernetes 1.37, containerd 2.3, gVisor extension 20260831.

Both used images built from Ubuntu 24.04 packages of DAOS 2.8 with the earlier version of the
patch.

| Scenario | kubeadm | Talos |
|---|---|---|
| dynamic PVC, RWX, pods with and without `hostUsers: false` sharing it (mount is `idmapped` in userns pods) | ✅ | ✅ |
| static PV for an existing container | ✅ | – |
| non-root pod in a user namespace, volume with `rootUid/rootGid/rootMode` | ✅ | ✅ |
| node plugin restart, mount-mode=pod: running pods keep their volumes | ✅ | ✅ |
| node plugin restart, mount-mode=process: running pods get ENOTCONN, new pods work | ✅ (as described) | – |
| `daos_agent` restart with `disable_auto_evict: true` (host service and DaemonSet) | ✅ | ✅ |
| libpil4dfs, `restricted` namespace, `hostUsers: false`, uid 1000 | ✅ | ✅ (with `daos-pod-nonat`) |
| gVisor pod, CSI volume (through the gofer) | ✅ | ✅ |
| gVisor pod, libpil4dfs with `D_IL_*` | ✅ | ✅ |
| gVisor + `hostUsers: false` | ❌ runsc cannot join the pod's user namespace (`setns ... invalid argument`), a gVisor limitation | ❌ same |
| pod deletion unstages (mount pod removed); PVC deletion destroys the container | ✅ | ✅ |

Throughput from `hack/bench.sh` on the kubeadm lab node (1 GiB, 4 MiB blocks, write with fsync).
The single DAOS server runs in a VM with emulated NVMe, so compare the rows, not the numbers:

| Access path | write MB/s | read MB/s |
|---|---|---|
| runc, dfuse (CSI volume) | 144 | 208 |
| runc, `hostUsers: false`, libpil4dfs | 250 | 417 |
| gVisor, dfuse through the gofer | 142 | 202 |
| gVisor, libpil4dfs (`D_IL_*`, netstack) | 121 | 276 |

With runsc `network = "host"`, libpil4dfs did not connect inside gVisor (0 intercepted calls).
This was not investigated further.
