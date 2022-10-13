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

	"golang.org/x/sync/errgroup"
)

var TheNetwork Network = &realNetwork{}

type ReplicateOpts struct {
	Fanout bool
}

func ReplicateObj(server string, k string, f *os.File, opts ReplicateOpts) error {
	return TheNetwork.ReplicateObj(server, k, f, opts)
}
func CheckObj(server string, k string) (ObjMeta, bool, error) {
	return TheNetwork.CheckObj(server, k)
}

// Represents the connection to outside nodes.
type Network interface {
	ReplicateObj(server string, k string, f *os.File, opts ReplicateOpts) error
	CheckObj(server string, k string) (ObjMeta, bool, error)
}

type realNetwork struct{}

var (
	ErrMisdirectedRequest  error = errors.New("misdirected request")
	ErrReplicationConflict error = errors.New("replication conflict")
)

func (network *realNetwork) ReplicateObj(server string, k string, f *os.File, opts ReplicateOpts) error {

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

	endpoint := fmt.Sprintf("%s/replicate?key=%s&fanout=%t", server, url.QueryEscape(k), opts.Fanout)
	resp, err := http.Post(endpoint, mpw.FormDataContentType(), r)
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

	if resp.StatusCode == http.StatusConflict {
		return ErrReplicationConflict
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

func (network *realNetwork) CheckObj(server string, k string) (ObjMeta, bool, error) {
	endpoint := fmt.Sprintf("%s/check?key=%s", server, url.QueryEscape(k))
	resp, err := http.Get(endpoint)
	if err != nil {
		return ObjMeta{}, false, fmt.Errorf("unable to check %q@%s: %w", k, server, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ObjMeta{}, false, fmt.Errorf("unable to read check body for %q@%s: %w", k, server, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ObjMeta{}, false, nil
	}

	if resp.StatusCode == http.StatusMisdirectedRequest {
		return ObjMeta{}, false, ErrMisdirectedRequest
	}

	if resp.StatusCode != http.StatusOK {
		return ObjMeta{}, false, fmt.Errorf("unable to check %q@%s: %s", k, server, resp.Status)
	}

	stat := ObjMeta{}
	err = json.Unmarshal(body, &stat)
	return stat, true, err
}

type MockNetwork struct {
	ReplicateFunc func(server string, k string, f *os.File, opts ReplicateOpts) error
	CheckFunc     func(server string, k string) (ObjMeta, bool, error)
}

func (network *MockNetwork) ReplicateObj(server string, k string, f *os.File, opts ReplicateOpts) error {
	return network.ReplicateFunc(server, k, f, opts)
}

func (network *MockNetwork) CheckObj(server string, k string) (ObjMeta, bool, error) {
	return network.CheckFunc(server, k)
}
