package driver

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

type mountState int

const (
	notMounted mountState = iota
	mounted
	// a FUSE mount whose daemon is gone: stat fails with ENOTCONN
	staleMount
)

type mountEntry struct {
	mountPoint string
	fsType     string
	source     string
	dev        string // maj:min; the same for every mount of one FUSE connection
	opts       string // per-mount options, e.g. "rw,nosuid"
}

func (e mountEntry) readOnly() bool {
	for _, o := range strings.Split(e.opts, ",") {
		if o == "ro" {
			return true
		}
	}
	return false
}

// mounts returns the entries of /proc/self/mountinfo whose mount point is path, in mount order.
func mounts(path string) ([]mountEntry, error) {
	ents, err := allMounts()
	if err != nil {
		return nil, err
	}
	var res []mountEntry
	for _, e := range ents {
		if e.mountPoint == path {
			res = append(res, e)
		}
	}
	return res, nil
}

// allMounts returns all entries of /proc/self/mountinfo, in mount order.
func allMounts() ([]mountEntry, error) {
	f, err := os.Open(mountinfoFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMountinfo(f)
}

func parseMountinfo(r io.Reader) ([]mountEntry, error) {
	var res []mountEntry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		// id parent maj:min root mountpoint opts [optional...] - fstype source superopts
		fields := strings.Fields(sc.Text())
		if len(fields) < 10 {
			continue
		}
		sep := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+2 >= len(fields) {
			continue
		}
		res = append(res, mountEntry{mountPoint: unescapeMountinfo(fields[4]),
			fsType: fields[sep+1], source: fields[sep+2], dev: fields[2], opts: fields[5]})
	}
	return res, sc.Err()
}

// unescapeMountinfo decodes the octal escapes (\040 etc.) used in mountinfo.
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// stateOf reports whether path is a mount point and whether it is usable. fsType is the type of
// the topmost mount.
func stateOf(path string) (mountState, string, error) {
	ents, err := mounts(path)
	if err != nil {
		return notMounted, "", err
	}
	if len(ents) == 0 {
		return notMounted, "", nil
	}
	top := ents[len(ents)-1]
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		if errors.Is(err, syscall.ENOTCONN) || errors.Is(err, syscall.EIO) {
			return staleMount, top.fsType, nil
		}
		return notMounted, top.fsType, err
	}
	return mounted, top.fsType, nil
}

// mounter is how the node service looks at and changes mounts; unit tests replace it.
type mounter interface {
	state(path string) (mountState, string, error)
	bind(src, dst string, readOnly bool) error
	unmountAll(path string) error
}

type sysMounter struct{}

func (sysMounter) state(path string) (mountState, string, error) { return stateOf(path) }
func (sysMounter) bind(src, dst string, ro bool) error           { return bindMount(src, dst, ro) }
func (sysMounter) unmountAll(path string) error                  { return unmountAll(path) }

func bindMount(src, dst string, readOnly bool) error {
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND, ""); err != nil {
		return err
	}
	if readOnly {
		flags := uintptr(syscall.MS_BIND | syscall.MS_REMOUNT | syscall.MS_RDONLY)
		if err := syscall.Mount("", dst, "", flags, ""); err != nil {
			_ = syscall.Unmount(dst, syscall.MNT_DETACH)
			return err
		}
	}
	return nil
}

// unmountAll removes every mount stacked on path, lazily if the plain unmount fails (e.g. a
// stale FUSE mount or one still in use).
func unmountAll(path string) error {
	for i := 0; i < 16; i++ {
		ents, err := mounts(path)
		if err != nil {
			return err
		}
		if len(ents) == 0 {
			return nil
		}
		if err := syscall.Unmount(path, 0); err != nil {
			if err := syscall.Unmount(path, syscall.MNT_DETACH); err != nil {
				return err
			}
		}
	}
	return errors.New("too many stacked mounts on " + path)
}
