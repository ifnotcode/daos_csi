# daos-csi image: the Go driver on top of an image with the DAOS client (daos CLI, dfuse built with
# FUSE_CAP_ALLOW_IDMAP, libpil4dfs).
#   BASE  image with the DAOS client installed: e.g. one built with Containerfile.client-ubuntu
#         from a client tarball, an Ubuntu image with daos-client .debs, or an EL9 image with the
#         daos-client RPMs
#   overlay/  optional files copied over the base (e.g. a patched usr/bin/dfuse); empty by default
# Docker looks for a Dockerfile only:  docker build -f Containerfile --build-arg BASE=... .
ARG BASE=localhost/daos-client-ubuntu:24.04

FROM docker.io/library/golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum* ./
COPY cmd cmd
COPY pkg pkg
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/daos-csi ./cmd/daos-csi && \
    cp go.mod go.sum /out/

FROM ${BASE}
RUN if command -v dnf >/dev/null; then dnf -y install fuse3 util-linux && dnf clean all; \
    else apt-get update && apt-get install -y --no-install-recommends fuse3 mount && \
         rm -rf /var/lib/apt/lists/*; fi
COPY overlay/ /
COPY --from=build /out/daos-csi /usr/bin/daos-csi
ENTRYPOINT ["/usr/bin/daos-csi"]
