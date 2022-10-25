package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"golang.org/x/sync/errgroup"
)

var TheNetwork Network = &realNetwork{}

type ReplicateOptions struct {
	Fanout bool
}

func ReplicateObj(cfg *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOptions) error {
	return TheNetwork.ReplicateObj(cfg, server, k, f, opts)
}
func CheckObj(cfg *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error) {
	return TheNetwork.CheckObj(cfg, server, k)
}

// Represents the connection to outside nodes.
type Network interface {
	ReplicateObj(cfg *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOptions) error
	CheckObj(cfg *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error)
}

type realNetwork struct{}

var (
	ErrMisdirectedRequest error = errors.New("misdirected request")
)

func (network *realNetwork) ReplicateObj(cfg *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOptions) error {

	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	mpw := multipart.NewWriter(w)
	errg, _ := errgroup.WithContext(context.Background())
	errg.Go(func() error {
		var part io.Writer
		defer w.Close()
		part, err := mpw.CreateFormFile("data", "data")
		if err != nil {
			return err
		}
		_, err = io.Copy(part, f)
		if err != nil {
			return err
		}
		err = mpw.Close()
		if err != nil {
			return err
		}
		return nil
	})
	defer func() {
		_ = r.Close()
		_ = errg.Wait()
	}()
	endpoint := fmt.Sprintf("%s/replicate?cid=%s&key=%s&fanout=%t", server, cfg.ConfigId, url.QueryEscape(k), opts.Fanout)
	req, err := http.NewRequest("POST", endpoint, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mpw.FormDataContentType())
	if cfg.ClusterSecret != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ClusterSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read response from %s: %s", endpoint, err)
	}

	if resp.StatusCode == http.StatusMisdirectedRequest {
		return ErrMisdirectedRequest
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replication of %q to %s failed: %s, body=%q", k, endpoint, resp.Status, body)
	}

	uploadErr := errg.Wait()
	if err != nil {
		return uploadErr
	}

	return nil
}

func (network *realNetwork) CheckObj(cfg *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error) {
	endpoint := fmt.Sprintf("%s/check?cid=%s&key=%s", server, cfg.ConfigId, url.QueryEscape(k))
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return ObjHeader{}, false, err
	}
	if cfg.ClusterSecret != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ClusterSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ObjHeader{}, false, fmt.Errorf("unable to check %q@%s: %w", k, server, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ObjHeader{}, false, fmt.Errorf("unable to read check body for %q@%s: %w", k, server, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ObjHeader{}, false, nil
	}

	if resp.StatusCode == http.StatusMisdirectedRequest {
		return ObjHeader{}, false, ErrMisdirectedRequest
	}

	if resp.StatusCode != http.StatusOK {
		return ObjHeader{}, false, fmt.Errorf("unable to check %q@%s: %s", k, server, resp.Status)
	}

	stat := ObjHeader{}
	err = json.Unmarshal(body, &stat)
	return stat, true, err
}

type MockNetwork struct {
	ReplicateFunc func(cfg *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOptions) error
	CheckFunc     func(cfg *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error)
}

func (network *MockNetwork) ReplicateObj(cfg *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOptions) error {
	return network.ReplicateFunc(cfg, server, k, f, opts)
}

func (network *MockNetwork) CheckObj(cfg *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error) {
	return network.CheckFunc(cfg, server, k)
}
