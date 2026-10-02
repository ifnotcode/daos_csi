#!/usr/bin/env bash
# Rebuild the daos-csi image on the lab node k8s-1 and roll it out (run from csi-daos/ on the Mac).
#   BASE   base image with the DAOS client (default: EL9 image + patched dfuse/pil4dfs overlay)
#   TAG    image tag (default: localhost/daos-csi:dev-el9)
set -euo pipefail

SSH="ssh -F ../testenv/ssh_config k8s-1"
BASE=${BASE:-localhost/daos-client:2.8.0}
TAG=${TAG:-localhost/daos-csi:dev-el9}

COPYFILE_DISABLE=1 tar czf - --no-xattrs --exclude=overlay --exclude=hack . |
	$SSH 'mkdir -p ~/csi-daos && tar xzf - -C ~/csi-daos'
$SSH "set -e; cd ~/csi-daos
	mkdir -p overlay/usr/bin overlay/usr/lib64
	if [ '$BASE' = localhost/daos-client:2.8.0 ]; then
		cp /usr/local/bin/dfuse-idmap overlay/usr/bin/dfuse
		cp /usr/local/lib64/libpil4dfs-idmap.so overlay/usr/lib64/libpil4dfs.so
	else
		rm -rf overlay/usr
		mkdir -p overlay
	fi
	sudo podman build -q --build-arg BASE=$BASE -t $TAG . >/dev/null
	sudo podman save --format oci-archive -o /tmp/daos-csi.tar $TAG
	sudo /usr/local/bin/ctr -n k8s.io images import /tmp/daos-csi.tar >/dev/null
	sudo rm -f /tmp/daos-csi.tar
	sed 's#DAOS_CSI_IMAGE#$TAG#' deploy/daos-csi.yaml > ~/daos-csi-dev.yaml
	kubectl apply -f ~/daos-csi-dev.yaml >/dev/null
	kubectl -n daos-csi rollout restart ds/daos-csi-node deploy/daos-csi-controller >/dev/null
	kubectl -n daos-csi rollout status ds/daos-csi-node --timeout=180s
	kubectl -n daos-csi rollout status deploy/daos-csi-controller --timeout=180s"
