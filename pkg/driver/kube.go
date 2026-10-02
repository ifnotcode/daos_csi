package driver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

var errNotFound = errors.New("not found")

// kubeClient is a minimal in-cluster client for the few pod operations the node plugin needs.
type kubeClient struct {
	base      string
	namespace string
	tokenFile string
	http      *http.Client
}

func newKubeClient() (*kubeClient, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster (KUBERNETES_SERVICE_HOST unset)")
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	ns, err := os.ReadFile(saDir + "/namespace")
	if err != nil {
		return nil, err
	}
	return &kubeClient{
		base:      "https://" + net.JoinHostPort(host, port),
		namespace: strings.TrimSpace(string(ns)),
		tokenFile: saDir + "/token",
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		},
	}, nil
}

func (k *kubeClient) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.base+path, rd)
	if err != nil {
		return err
	}
	// Re-read the token each time: projected tokens are rotated.
	tok, err := os.ReadFile(k.tokenFile)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := k.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (k *kubeClient) podPath(name string) string {
	p := "/api/v1/namespaces/" + k.namespace + "/pods"
	if name != "" {
		p += "/" + name
	}
	return p
}

type podStatus struct {
	Metadata struct {
		Name              string            `json:"name"`
		DeletionTimestamp *string           `json:"deletionTimestamp"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Status struct {
		Phase             string `json:"phase"`
		Reason            string `json:"reason"`
		Message           string `json:"message"`
		ContainerStatuses []struct {
			RestartCount int `json:"restartCount"`
			State        struct {
				Waiting *struct {
					Reason  string `json:"reason"`
					Message string `json:"message"`
				} `json:"waiting"`
				Terminated *struct {
					Reason   string `json:"reason"`
					Message  string `json:"message"`
					ExitCode int    `json:"exitCode"`
				} `json:"terminated"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (k *kubeClient) getPod(ctx context.Context, name string) (*podStatus, error) {
	var p podStatus
	if err := k.do(ctx, http.MethodGet, k.podPath(name), nil, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (k *kubeClient) createPod(ctx context.Context, pod map[string]any) error {
	return k.do(ctx, http.MethodPost, k.podPath(""), pod, nil)
}

func (k *kubeClient) deletePod(ctx context.Context, name string) error {
	err := k.do(ctx, http.MethodDelete, k.podPath(name), nil, nil)
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}
