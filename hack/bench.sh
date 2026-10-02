#!/usr/bin/env bash
# Runs examples/bench.yaml pods one at a time and prints their throughput.
#   IMAGE  DAOS client image (default: the driver image); SC  StorageClass (default: daos-uid1000,
#   whose ACL admits pods in user namespaces)
set -uo pipefail
EX=${EX:-$(dirname "$0")/../examples}
IMAGE=${IMAGE:-$(kubectl -n daos-csi get ds daos-csi-node -o jsonpath='{.spec.template.spec.containers[1].image}')}
SC=${SC:-daos-uid1000}

kubectl delete pod bench-runc bench-runc-pil bench-gvisor bench-gvisor-pil --ignore-not-found --wait=true >/dev/null
kubectl delete pvc bench --ignore-not-found --wait=true >/dev/null
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: bench}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $SC
  resources: {requests: {storage: 10Gi}}
EOF
for _ in $(seq 1 60); do [ "$(kubectl get pvc bench -o jsonpath='{.status.phase}')" = Bound ] && break; sleep 2; done
cont=$(kubectl get pv "$(kubectl get pvc bench -o jsonpath='{.spec.volumeName}')" -o jsonpath='{.spec.csi.volumeHandle}')
cont=${cont#*/}

manifest=$(sed -e "s#DAOS_CLIENT_IMAGE#$IMAGE#" -e "s#BENCH_CONTAINER#$cont#" "$EX/bench.yaml")
echo "$manifest" | awk 'BEGIN{RS="---\n"} NR==1' | kubectl apply -f - >/dev/null
for p in bench-runc bench-runc-pil bench-gvisor bench-gvisor-pil; do
	echo "$manifest" | awk -v p="$p" 'BEGIN{RS="---\n"} $0 ~ "name: " p "}" {print}' | kubectl apply -f - >/dev/null
	for _ in $(seq 1 150); do kubectl logs "$p" 2>/dev/null | grep -q DONE && break; sleep 2; done
	echo "== $p"
	kubectl logs "$p" 2>/dev/null | grep -E "copied|\[(read|write)" | sed 's/^/   /'
	kubectl delete pod "$p" --wait=true >/dev/null
done
kubectl delete pvc bench --wait=false >/dev/null
