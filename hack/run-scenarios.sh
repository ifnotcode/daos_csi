#!/usr/bin/env bash
# Runs the csi-daos scenario suite on a cluster (run where kubectl works, e.g. on k8s-1).
#   IMAGE      DAOS client image for application pods (with libpil4dfs), default: the driver image
#   GVISOR=1   also run the gVisor scenarios (needs RuntimeClass gvisor, runsc host-uds=open)
# Expects: driver deployed (deploy/daos-csi.yaml), StorageClasses "daos" and "daos-uid1000"
# (examples/storageclass-nonroot.yaml without its PVC/pod), daos_agent on every node.
set -uo pipefail

EX=${EX:-$(dirname "$0")/../examples}
IMAGE=${IMAGE:-$(kubectl -n daos-csi get ds daos-csi-node -o jsonpath='{.spec.template.spec.containers[1].image}')}
GVISOR=${GVISOR:-0}
# daos CLI for the container-count check; on nodes without it (e.g. Talos) run it in the controller:
#   DAOS="kubectl -n daos-csi exec deploy/daos-csi-controller -c daos-csi -- daos"
DAOS=${DAOS:-daos}
results=()
res() { results+=("$(printf '%-44s %s' "$1" "$2")"); echo "   -> $1: $2"; }

wait_log() { # ns pod pattern timeout
	for _ in $(seq 1 "$4"); do
		kubectl -n "$1" logs "$2" 2>/dev/null | grep -q "$3" && return 0
		sleep 2
	done
	return 1
}

cleanup() {
	# pods first: a PVC is only deleted once no pod uses it
	kubectl delete pod csi-nonroot-owned gv-volume gv-volume-userns gv-pil4dfs --ignore-not-found \
		--wait=true >/dev/null 2>&1
	kubectl delete -f "$EX/pvc-userns.yaml" --ignore-not-found --wait=true >/dev/null 2>&1
	kubectl delete ns daos-apps --ignore-not-found --wait=true >/dev/null 2>&1
	kubectl delete pvc daos-uid1000 --ignore-not-found --wait=true >/dev/null 2>&1
}

echo "== image: $IMAGE"
cleanup

echo "== 1. dynamic RWX volume shared by userns / hostusers / userns non-root pods"
kubectl apply -f "$EX/pvc-userns.yaml" >/dev/null
kubectl wait --for=condition=Ready pod/csi-userns pod/csi-hostusers pod/csi-userns-nonroot \
	--timeout=180s >/dev/null 2>&1
kubectl logs csi-userns | grep -q "fuse.daos .*idmapped" && res "userns pod gets an idmapped dfuse mount" OK ||
	res "userns pod gets an idmapped dfuse mount" FAIL
kubectl exec csi-hostusers -- cat /data/userns.txt 2>/dev/null | grep -q from-userns &&
	res "data written in userns pod visible to others" OK || res "data written in userns pod visible to others" FAIL
kubectl logs csi-userns-nonroot | grep -q "Permission denied" &&
	res "uid 1000 denied on root-owned volume root" "OK (expected)" || res "uid 1000 denied on root-owned volume root" FAIL

echo "== 2. rootUid/rootGid/rootMode volume, non-root userns pod"
sed '/^---$/,$d' "$EX/storageclass-nonroot.yaml" | kubectl apply -f - >/dev/null 2>&1
awk 'BEGIN{RS="---\n"} NR>1 {printf "---\n%s", $0}' "$EX/storageclass-nonroot.yaml" | kubectl apply -f - >/dev/null
kubectl wait --for=condition=Ready pod/csi-nonroot-owned --timeout=180s >/dev/null 2>&1
kubectl logs csi-nonroot-owned | grep -q "1000 *1000 .*f.txt" && res "non-root pod writes to rootUid volume" OK ||
	res "non-root pod writes to rootUid volume" FAIL

echo "== 3. node plugin restart (mount pods keep volumes alive)"
kubectl -n daos-csi rollout restart ds/daos-csi-node >/dev/null
kubectl -n daos-csi rollout status ds/daos-csi-node --timeout=180s >/dev/null
kubectl exec csi-userns -- sh -c "echo x > /data/after-plugin-restart" 2>/dev/null &&
	res "volume survives node plugin restart" OK || res "volume survives node plugin restart" FAIL

echo "== 4. daos_agent restart (disable_auto_evict)"
if kubectl -n daos-csi get ds daos-agent >/dev/null 2>&1; then
	kubectl -n daos-csi rollout restart ds/daos-agent >/dev/null
	kubectl -n daos-csi rollout status ds/daos-agent --timeout=180s >/dev/null
	kubectl exec csi-userns -- sh -c "echo x > /data/after-agent-restart" 2>/dev/null &&
		res "volume survives daos_agent restart" OK || res "volume survives daos_agent restart" FAIL
else
	res "volume survives daos_agent restart" "SKIPPED (agent not a DaemonSet)"
fi

echo "== 5. libpil4dfs, restricted namespace, hostUsers: false, uid 1000"
sed "s#DAOS_CLIENT_IMAGE#$IMAGE#" "$EX/pil4dfs-restricted.yaml" | kubectl apply -f - >/dev/null
if wait_log daos-apps pil4dfs-restricted DONE 120; then
	# "[write  ]  64" / "[read   ]  65" (not "[readdir]")
	w=$(kubectl -n daos-apps logs pil4dfs-restricted | awk '/^\[write +\]/ {print $3; exit}')
	r=$(kubectl -n daos-apps logs pil4dfs-restricted | awk '/^\[read +\]/ {v=$3} END {print v}')
	[ "${w:-0}" -gt 0 ] && [ "${r:-0}" -gt 0 ] && res "libpil4dfs intercepts in restricted userns pod" "OK (w=$w r=$r)" ||
		res "libpil4dfs intercepts in restricted userns pod" "FAIL (w=$w r=$r)"
else
	res "libpil4dfs intercepts in restricted userns pod" "FAIL (pod did not finish)"
fi

if [ "$GVISOR" = 1 ]; then
	echo "== 6. gVisor"
	vh=$(kubectl get pv "$(kubectl get pvc daos-dyn -o jsonpath='{.spec.volumeName}')" \
		-o jsonpath='{.spec.csi.volumeHandle}')
	sed -e "s#DAOS_CLIENT_IMAGE#$IMAGE#" -e "s#DAOS_CONTAINER#${vh#*/}#" "$EX/gvisor.yaml" |
		kubectl apply -f - >/dev/null
	wait_log default gv-volume DONE 90 && res "gVisor pod uses CSI volume" OK || res "gVisor pod uses CSI volume" FAIL
	if wait_log default gv-pil4dfs DONE 120; then
		kubectl logs gv-pil4dfs | awk '/D_IL_MOUNT_POINT/ {f=1} f && /^\[write +\]/ {print $3}' | grep -qv '^0$' &&
			res "gVisor libpil4dfs (D_IL_*) intercepts" OK || res "gVisor libpil4dfs (D_IL_*) intercepts" FAIL
	else
		res "gVisor libpil4dfs (D_IL_*) intercepts" "FAIL (pod did not finish)"
	fi
	sleep 20
	kubectl get pod gv-volume-userns -o jsonpath='{.status.phase}' | grep -q Running &&
		res "gVisor + hostUsers: false" "OK" || res "gVisor + hostUsers: false" "FAILS (known runsc limitation)"
fi

echo "== 7. teardown: unstage (mount pods removed) and container deletion"
before=$($DAOS container list tank 2>/dev/null | grep -c pvc- || true)
cleanup
sleep 10
left=$(kubectl -n daos-csi get pods -l app=daos-csi-mount --no-headers 2>/dev/null | wc -l)
after=$($DAOS container list tank 2>/dev/null | grep -c pvc- || true)
[ "$left" = 0 ] && res "all mount pods removed after pods deleted" OK || res "all mount pods removed after pods deleted" "FAIL ($left left)"
[ "$after" -lt "$before" ] && res "PVC deletion destroys DAOS containers" "OK ($before -> $after)" ||
	res "PVC deletion destroys DAOS containers" "FAIL ($before -> $after)"

echo
echo "================ summary"
printf '%s\n' "${results[@]}"
