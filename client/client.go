package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"golang.org/x/sync/errgroup"
)

type ClientRequestError struct {
	Server     string
	StatusCode int
	Redirect   string
	Body       string
}

func (e *ClientRequestError) Error() string {
	return fmt.Sprintf("error reponse from %s: %d - %s", e.Server, e.StatusCode, e.Body)
}

type MultiError struct {
	Errors []error
}

func (e *MultiError) Error() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s and %d other errors", e.Errors[0], len(e.Errors)-1)
	return buf.String()
}

type Client struct {
	http          *http.Client
	configWatcher clusterconfig.ConfigWatcher
}

type ClientOptions struct {
	OnConfigChange func(cfg *clusterconfig.ClusterConfig, err error)
}

func New(configPath string, opts ClientOptions) (*Client, error) {

	if opts.OnConfigChange == nil {
		opts.OnConfigChange = func(cfg *clusterconfig.ClusterConfig, err error) {
			if err != nil {
				log.Printf("error reloading client config: %s", err)
			}
		}
	}

	configWatcher, err := clusterconfig.NewConfigFileWatcher(configPath)
	if err != nil {
		return nil, fmt.Errorf("error loading initial config: %w", err)
	}
	configWatcher.OnConfigChange(opts.OnConfigChange)

	return &Client{
		http:          &http.Client{},
		configWatcher: configWatcher,
	}, nil
}

func (c *Client) Close() error {
	c.configWatcher.Stop()
	return nil
}

func (c *Client) GetClusterConfig() *clusterconfig.ClusterConfig {
	return c.configWatcher.GetCurrentConfig()
}

func responseError(server string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return &ClientRequestError{
		Server:     server,
		StatusCode: resp.StatusCode,
		Body:       string(body),
	}
}

func (c *Client) setAuthHeaders(cfg *clusterconfig.ClusterConfig, req *http.Request) {
	if cfg.ClusterSecret != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ClusterSecret)
	}
}

func (c *Client) httpGet(cfg *clusterconfig.ClusterConfig, endpoint string) (*http.Response, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.setAuthHeaders(cfg, req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) post(cfg *clusterconfig.ClusterConfig, endpoint, contentType string, r io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	c.setAuthHeaders(cfg, req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) postForm(cfg *clusterconfig.ClusterConfig, endpoint string, values url.Values) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c.setAuthHeaders(cfg, req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) put(cfg *clusterconfig.ClusterConfig, server string, k string, data io.Reader, opts PutOptions) error {
	r, w := io.Pipe()
	mpw := multipart.NewWriter(w)
	contentType := mpw.FormDataContentType()
	errg, _ := errgroup.WithContext(context.Background())
	errg.Go(func() error {
		var part io.Writer
		defer w.Close()
		part, err := mpw.CreateFormFile("data", "data")
		if err != nil {
			return err
		}
		_, err = io.Copy(part, data)
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

	replicas := ""
	if opts.Replicas > 0 {
		replicas = fmt.Sprintf("&replicas=%d", opts.Replicas)
	}

	endpoint := fmt.Sprintf("%s/put?key=%s%s", server, url.QueryEscape(k), replicas)
	resp, err := c.post(cfg, endpoint, contentType, r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return responseError(server, resp)
	}

	uploadErr := errg.Wait()
	if err != nil {
		return uploadErr
	}

	return nil
}

type PutOptions struct {
	Replicas uint
}

func (c *Client) Put(k string, data io.ReadSeeker, opts PutOptions) error {
	cfg := c.GetClusterConfig()
	locs, err := cfg.Crush(k)
	if err != nil {
		return err
	}

	errs := []error{}
	for i, loc := range locs {
		server := loc[len(loc)-1]
		if i != 0 {
			_, err := data.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}
		}
		err = c.put(cfg, server, k, data, opts)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return &MultiError{Errors: errs}
	}
}

type ObjectReader struct {
	resp *http.Response
}

func (or *ObjectReader) Read(buf []byte) (int, error) {
	return or.resp.Body.Read(buf)
}

func (or *ObjectReader) Close() error {
	return or.resp.Body.Close()
}

func (c *Client) get(cfg *clusterconfig.ClusterConfig, server string, k string, opts GetOptions) (*ObjectReader, error) {
	endpoint := fmt.Sprintf("%s/get?key=%s", server, url.QueryEscape(k))
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.setAuthHeaders(cfg, req)
	if opts.StartOffset != 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", opts.StartOffset))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		err := responseError(server, resp)
		_ = resp.Body.Close()
		return nil, err
	}
	return &ObjectReader{resp: resp}, nil
}

type GetOptions struct {
	StartOffset uint64
}

func (c *Client) Get(k string, opts GetOptions) (*ObjectReader, bool, error) {
	cfg := c.GetClusterConfig()
	locs, err := cfg.Crush(k)
	if err != nil {
		return nil, false, err
	}
	errs := []error{}
	for _, loc := range locs {
		server := loc[len(loc)-1]
		objReader, err := c.get(cfg, server, k, opts)
		if err != nil {
			if err, ok := err.(*ClientRequestError); ok {
				if err.StatusCode == http.StatusNotFound {
					continue
				}
				if err.StatusCode == http.StatusGone {
					return nil, false, nil
				}
			}
			errs = append(errs, err)
			break
		}
		return objReader, true, nil
	}
	switch len(errs) {
	case 0:
		return nil, false, nil
	case 1:
		return nil, false, errs[0]
	default:
		return nil, false, &MultiError{Errors: errs}
	}
}

type ObjectHeader struct {
	CreatedAtUnixMicro uint64
	Size               uint64
	B3sum              string
}

func (c *Client) head(cfg *clusterconfig.ClusterConfig, server string, k string, opts HeadOptions) (ObjectHeader, error) {
	endpoint := fmt.Sprintf("%s/head?key=%s", server, url.QueryEscape(k))
	resp, err := c.httpGet(cfg, endpoint)
	if err != nil {
		return ObjectHeader{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ObjectHeader{}, responseError(server, resp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ObjectHeader{}, err
	}

	header := ObjectHeader{}
	err = json.Unmarshal(body, &header)
	if err != nil {
		return ObjectHeader{}, err
	}

	return header, nil
}

type HeadOptions struct{}

func (c *Client) Head(k string, opts HeadOptions) (ObjectHeader, bool, error) {

	cfg := c.GetClusterConfig()
	locs, err := cfg.Crush(k)
	if err != nil {
		return ObjectHeader{}, false, err
	}

	errs := []error{}
	for _, loc := range locs {
		server := loc[len(loc)-1]
		header, err := c.head(cfg, server, k, opts)
		if err != nil {
			if err, ok := err.(*ClientRequestError); ok {
				if err.StatusCode == http.StatusNotFound {
					continue
				}
				if err.StatusCode == http.StatusGone {
					return ObjectHeader{}, false, nil
				}
			}
			errs = append(errs, err)
			break
		}
		return header, true, nil
	}
	switch len(errs) {
	case 0:
		return ObjectHeader{}, false, nil
	case 1:
		return ObjectHeader{}, false, errs[0]
	default:
		return ObjectHeader{}, false, &MultiError{Errors: errs}
	}
}

func (c *Client) delete(cfg *clusterconfig.ClusterConfig, server string, k string, opts DeleteOptions) error {
	endpoint := fmt.Sprintf("%s/delete?key=%s", server, url.QueryEscape(k))
	resp, err := c.postForm(cfg, endpoint, url.Values{})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return responseError(server, resp)
	}
	return nil
}

type DeleteOptions struct {
}

func (c *Client) Delete(k string, opts DeleteOptions) error {
	cfg := c.GetClusterConfig()
	locs, err := cfg.Crush(k)
	if err != nil {
		return err
	}
	// Delete at the primary server.
	server := locs[0][len(locs[0])-1]
	err = c.delete(cfg, server, k, opts)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) iterBegin(cfg *clusterconfig.ClusterConfig, server string, typ string) (string, error) {
	endpoint := fmt.Sprintf("%s/iter_begin?type=%s", server, url.QueryEscape(typ))
	resp, err := c.postForm(cfg, endpoint, url.Values{})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", responseError(server, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	cursor := ""
	err = json.Unmarshal(body, &cursor)
	if err != nil {
		return "", err
	}
	return cursor, nil
}

func (c *Client) iterNext(cfg *clusterconfig.ClusterConfig, server string, cursor string, iterOut interface{}) error {
	endpoint := fmt.Sprintf("%s/iter_next?it=%s", server, url.QueryEscape(cursor))
	resp, err := c.postForm(cfg, endpoint, url.Values{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(server, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, iterOut)
	if err != nil {
		return err
	}
	return nil
}

type RemoteObject struct {
	Server    string
	Key       string
	Size      uint64
	Tombstone bool
	B3sum     string
	CreatedAt time.Time
}

type ListOptions struct {
	Deleted bool
}

func (c *Client) List(cb func(RemoteObject) bool, opts ListOptions) error {
	cfg := c.GetClusterConfig()
	for _, node := range cfg.StorageHierarchy.StorageNodes {
		if node.IsDefunct() {
			continue
		}
		server := node.Location[len(node.Location)-1]
		cursor, err := c.iterBegin(cfg, server, "objects")
		if err != nil {
			return err
		}
		objects := []struct {
			Key                string
			B3sum              string
			Size               uint64
			Tombstone          bool
			CreatedAtUnixMicro uint64
		}{}
		for {
			err := c.iterNext(cfg, server, cursor, &objects)
			if err != nil {
				return err
			}
			if len(objects) == 0 {
				break
			}
			for _, o := range objects {
				if o.Tombstone && !opts.Deleted {
					continue
				}
				cont := cb(RemoteObject{
					Server:    server,
					Key:       o.Key,
					Size:      o.Size,
					Tombstone: o.Tombstone,
					B3sum:     o.B3sum,
					CreatedAt: time.UnixMicro(int64(o.CreatedAtUnixMicro)),
				})
				if !cont {
					return nil
				}
			}
		}
	}

	return nil
}

type RemoteKey struct {
	Server string
	Key    string
}

type ListKeysOptions struct {
}

func (c *Client) ListKeys(cb func(RemoteKey) bool, opts ListKeysOptions) error {
	cfg := c.GetClusterConfig()
	for _, node := range cfg.StorageHierarchy.StorageNodes {
		if node.IsDefunct() {
			continue
		}
		server := node.Location[len(node.Location)-1]
		cursor, err := c.iterBegin(cfg, server, "keys")
		if err != nil {
			return err
		}
		keys := []string{}
		for {
			err := c.iterNext(cfg, server, cursor, &keys)
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				break
			}
			for _, k := range keys {
				if !cb(RemoteKey{Server: server, Key: k}) {
					return nil
				}
			}
		}
	}

	return nil
}

type ClusterStatusOptions struct {
	QueryDefunct bool
}

type NodeInfo struct {
	HeapAlloc                          uint64
	FreeSpace                          uint64
	UsedSpace                          uint64
	FreeRAM                            uint64
	TotalScrubbedObjects               uint64
	TotalScrubCorruptionErrorCount     uint64
	TotalScrubReplicationErrorCount    uint64
	TotalScrubOtherErrorCount          uint64
	LastScrubStartingConfigId          string
	LastScrubUnixMicro                 uint64
	LastScrubCorruptionErrorCount      uint64
	LastScrubReplicationErrorCount     uint64
	LastScrubOtherErrorCount           uint64
	LastFullScrubUnixMicro             uint64
	LastScrubDuration                  time.Duration
	LastFullScrubDuration              time.Duration
	LastFullScrubCorruptionErrorCount  uint64
	LastFullScrubReplicationErrorCount uint64
	LastFullScrubOtherErrorCount       uint64
	LastScrubObjects                   uint64
}

func (c *Client) nodeInfo(cfg *clusterconfig.ClusterConfig, server string) (NodeInfo, error) {
	endpoint := fmt.Sprintf("%s/node_info", server)
	resp, err := c.httpGet(cfg, endpoint)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("unable to request node info from %s: %w", server, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return NodeInfo{}, responseError(server, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("unable to read node info from %s: %w", server, err)
	}
	ni := NodeInfo{}
	err = json.Unmarshal(body, &ni)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("unable to unmarshal node info from %s: %w", server, err)
	}
	return ni, nil
}

type ClusterStatus struct {
	Unreachable []string
	Errors      []error
	Nodes       []string
	NodeInfo    map[string]NodeInfo
}

func (c *Client) ClusterStatus(opts ClusterStatusOptions) ClusterStatus {
	cfg := c.GetClusterConfig()

	status := ClusterStatus{
		NodeInfo: make(map[string]NodeInfo),
	}

	for _, node := range cfg.StorageHierarchy.StorageNodes {
		if node.IsDefunct() && !opts.QueryDefunct {
			continue
		}
		loc := node.Location
		server := loc[len(loc)-1]
		status.Nodes = append(status.Nodes, server)
		nodeInfo, err := c.nodeInfo(cfg, server)
		if err != nil {
			status.Unreachable = append(status.Unreachable, server)
			status.Errors = append(status.Errors, err)
		} else {
			status.NodeInfo[server] = nodeInfo
		}
	}

	return status
}

type ScrubClusterOptions struct {
	PollInterval time.Duration
	FullScrub    bool
}

type ScrubClusterProgress struct {
	ApproximatePercentComplete float64
	NodesRemaining             uint64
	ScrubbedObjects            uint64
	ReplicationErrors          uint64
	CorruptionErrors           uint64
	OtherErrors                uint64
}

func (c *Client) ScrubCluster(OnProgress func(ScrubClusterProgress), opts ScrubClusterOptions) error {

	if opts.PollInterval == 0 {
		opts.PollInterval = 2 * time.Second
	}

	startTimeUnixMicro := uint64(time.Now().UnixMicro())
	// Sleep to help take into account clock drift in the cluster,
	// this machine must be less than two seconds ahead.
	time.Sleep(2 * time.Second)

	lastScrubTotalObjects := uint64(0)
	startScrubbedObjects := uint64(0)
	startReplicationErrors := uint64(0)
	startCorruptionErrors := uint64(0)
	startOtherErrors := uint64(0)

	cfg := c.GetClusterConfig()
	for _, node := range cfg.StorageHierarchy.StorageNodes {
		if node.IsDefunct() {
			continue
		}
		loc := node.Location
		server := loc[len(loc)-1]

		nodeInfo, err := c.nodeInfo(cfg, server)
		if err != nil {
			return fmt.Errorf("unable to query scrub info for %s: %w", server, err)
		}
		lastScrubTotalObjects += nodeInfo.LastScrubObjects
		startScrubbedObjects += nodeInfo.TotalScrubbedObjects
		startReplicationErrors += nodeInfo.TotalScrubReplicationErrorCount
		startCorruptionErrors += nodeInfo.TotalScrubCorruptionErrorCount
		startOtherErrors += nodeInfo.TotalScrubOtherErrorCount
		endpoint := fmt.Sprintf("%s/start_scrub?full=%v", server, opts.FullScrub)
		_, err = c.postForm(cfg, endpoint, url.Values{})
		if err != nil {
			return fmt.Errorf("unable to start scrub of %s: %w", server, err)
		}
	}

	for {
		nodesRemaining := uint64(0)
		scrubbedObjects := uint64(0)
		replicationErrors := uint64(0)
		corruptionErrors := uint64(0)
		otherErrors := uint64(0)

		cfg = c.GetClusterConfig()
		for _, node := range cfg.StorageHierarchy.StorageNodes {
			if node.IsDefunct() {
				continue
			}
			loc := node.Location
			server := loc[len(loc)-1]
			nodeInfo, err := c.nodeInfo(cfg, server)
			if err != nil {
				return fmt.Errorf("unable to query scrub info for %s: %w", server, err)
			}

			if opts.FullScrub {
				if nodeInfo.LastFullScrubUnixMicro < startTimeUnixMicro {
					nodesRemaining += 1
				}
			} else {
				if nodeInfo.LastScrubUnixMicro < startTimeUnixMicro {
					nodesRemaining += 1
				}
			}
			scrubbedObjects += nodeInfo.TotalScrubbedObjects
			replicationErrors += nodeInfo.TotalScrubReplicationErrorCount
			corruptionErrors += nodeInfo.TotalScrubCorruptionErrorCount
			otherErrors += nodeInfo.TotalScrubOtherErrorCount
		}
		OnProgress(ScrubClusterProgress{
			ApproximatePercentComplete: (float64(scrubbedObjects-startScrubbedObjects) / float64(lastScrubTotalObjects)) * 100,
			NodesRemaining:             nodesRemaining,
			ScrubbedObjects:            scrubbedObjects - startScrubbedObjects,
			ReplicationErrors:          replicationErrors - startReplicationErrors,
			CorruptionErrors:           corruptionErrors - startCorruptionErrors,
			OtherErrors:                replicationErrors - startReplicationErrors,
		})
		if nodesRemaining == 0 {
			break
		}
		time.Sleep(opts.PollInterval)
	}

	return nil
}
