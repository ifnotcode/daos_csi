package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (d *Driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: []*csi.ControllerServiceCapability{{
		Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{
			Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		}},
	}}}, nil
}

// CreateVolume creates a DAOS POSIX container labelled with the volume name in the pool given
// by the StorageClass parameter "pool". DAOS containers have no size limit of their own, so the
// requested capacity is only echoed back.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := checkCaps(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	if req.GetVolumeContentSource() != nil {
		return nil, status.Error(codes.InvalidArgument, "volume content source is not supported")
	}
	params := req.GetParameters()
	pool := params[ParamPool]
	if pool == "" {
		return nil, status.Error(codes.InvalidArgument, "StorageClass parameter \"pool\" is required")
	}

	// The node only reads these when it stages the volume: catch mistakes now
	if err := (podResources{}).withVolume(params).validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "StorageClass parameter %v", err)
	}

	unlock := d.locks.lock(pool + "/" + name)
	defer unlock()

	args := []string{"container", "create", pool, name, "--type=POSIX"}
	for key, flag := range map[string]string{
		ParamContProps: "--properties", ParamContOclass: "--oclass",
		ParamContDirClass: "--dir-oclass", ParamContFileCls: "--file-oclass",
		ParamChunkSize: "--chunk-size", ParamContGroup: "--group",
	} {
		if v := params[key]; v != "" {
			args = append(args, flag+"="+v)
		}
	}
	if v := params[ParamContACL]; v != "" {
		f, err := writeACLFile(v)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "writing ACL file: %v", err)
		}
		defer os.Remove(f)
		args = append(args, "--acl-file="+f)
	}
	out, err := d.daos(ctx, args...)
	if err != nil && !isDerExist(out, err) {
		return nil, status.Errorf(codes.Internal, "daos container create: %v", err)
	}

	volCtx := map[string]string{ParamPool: pool, ParamContainer: name}
	for _, k := range append([]string{ParamDfuseArgs, ParamRootUID, ParamRootGID, ParamRootMode},
		resourceParams...) {
		if v := params[k]; v != "" {
			volCtx[k] = v
		}
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      pool + "/" + name,
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
		VolumeContext: volCtx,
	}}, nil
}

// DeleteVolume destroys the container. It only runs for reclaimPolicy: Delete; use Retain on the
// StorageClass or PV to keep the data.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	pool, cont, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		// Not ours or malformed: nothing to delete
		return &csi.DeleteVolumeResponse{}, nil
	}
	unlock := d.locks.lock(pool + "/" + cont)
	defer unlock()

	out, err := d.daos(ctx, "container", "destroy", pool, cont)
	if err != nil && !isDerNonexist(out, err) {
		if strings.Contains(out, "DER_BUSY") {
			return nil, status.Errorf(codes.FailedPrecondition,
				"container %s/%s is still open (dfuse still mounted?): %v", pool, cont, err)
		}
		return nil, status.Errorf(codes.Internal, "daos container destroy: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	pool, cont, err := parseVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if out, err := d.daos(ctx, "container", "query", pool, cont); err != nil {
		if isDerNonexist(out, err) {
			return nil, status.Errorf(codes.NotFound, "container %s/%s not found", pool, cont)
		}
		return nil, status.Errorf(codes.Internal, "daos container query: %v", err)
	}
	if err := checkCaps(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeContext:      req.GetVolumeContext(),
		VolumeCapabilities: req.GetVolumeCapabilities(),
		Parameters:         req.GetParameters(),
	}}, nil
}

// checkCaps accepts filesystem volumes in any access mode: a DAOS container can be mounted on
// many nodes at once.
func checkCaps(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if c.GetBlock() != nil {
			return status.Error(codes.InvalidArgument, "block volumes are not supported")
		}
	}
	return nil
}

// daos runs the daos CLI with JSON output and returns its combined output.
func (d *Driver) daos(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.cfg.DaosBin, append([]string{"-j"}, args...)...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	out := buf.String()
	if err == nil {
		// daos -j reports failures in the JSON body as well
		var r struct {
			Error  *string `json:"error"`
			Status int     `json:"status"`
		}
		if json.Unmarshal([]byte(out), &r) == nil && (r.Status != 0 || r.Error != nil) {
			msg := ""
			if r.Error != nil {
				msg = *r.Error
			}
			err = fmt.Errorf("status %d: %s", r.Status, msg)
		}
	}
	if err != nil {
		return out, fmt.Errorf("daos %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return out, nil
}

// writeACLFile writes the comma separated ACL entries to a temporary file for --acl-file, adding
// the default owner and owner-group entries unless they are given.
func writeACLFile(entries string) (string, error) {
	var lines []string
	hasOwner, hasGroup := false, false
	for _, e := range strings.FieldsFunc(entries, func(r rune) bool { return r == ',' || r == '\n' }) {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		hasOwner = hasOwner || strings.Contains(e, ":OWNER@:")
		hasGroup = hasGroup || strings.Contains(e, ":GROUP@:")
		lines = append(lines, e)
	}
	if !hasOwner {
		lines = append([]string{"A::OWNER@:rwdtTaAo"}, lines...)
	}
	if !hasGroup {
		lines = append(lines, "A:G:GROUP@:rwtT")
	}
	f, err := os.CreateTemp("", "daos-csi-acl-*")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func isDerExist(out string, err error) bool {
	return err != nil && (strings.Contains(out, "DER_EXIST") || strings.Contains(out, "already exists"))
}

func isDerNonexist(out string, err error) bool {
	return err != nil && (strings.Contains(out, "DER_NONEXIST") || strings.Contains(out, "not found") ||
		strings.Contains(out, "does not exist"))
}
