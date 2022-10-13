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
	"time"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"golang.org/x/sync/errgroup"
)

const MAX_REDIRECTS = 5

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
		http: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to response from %s: %s", resp.Request.URL, err)
	}
	redirect := ""
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		redirectLoc, _ := resp.Header["Location"]
		if len(redirectLoc) > 0 {
			u, err := url.Parse(redirectLoc[0])
			if err == nil {
				port := u.Port()
				if port != "" {
					port = ":" + port
				}
				redirect = fmt.Sprintf("%s://%s%s", u.Scheme, u.Hostname(), port)
			}
		}
	}
	return &ClientRequestError{
		Server:     server,
		Redirect:   redirect,
		StatusCode: resp.StatusCode,
		Body:       string(body),
	}
}

func (c *Client) _put(server string, k string, data io.Reader, opts PutOptions) error {
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

	endpoint := fmt.Sprintf("%s/put?key=%s", server, url.QueryEscape(k))
	resp, err := c.http.Post(endpoint, contentType, r)
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
}

func (c *Client) Put(k string, data io.ReadSeeker, opts PutOptions) error {
	locs, err := c.GetClusterConfig().Crush(k)
	if err != nil {
		return err
	}

	// Upload to the primary server.
	server := locs[0][len(locs[0])-1]
	redirects := 0
	for {
		if redirects != 0 {
			_, err := data.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}
		}
		err := c._put(server, k, data, opts)
		if err != nil {
			if err, ok := err.(*ClientRequestError); ok && err.Redirect != "" && redirects < MAX_REDIRECTS {
				server = err.Redirect
				redirects += 1
				continue
			}
			return err
		}
		return nil
	}
}

func (c *Client) get(server string, k string, into io.Writer, opts GetOptions) error {

	endpoint := fmt.Sprintf("%s/get?key=%s", server, url.QueryEscape(k))
	resp, err := c.http.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return responseError(server, resp)
	}

	_, err = io.Copy(into, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

type GetOptions struct {
	KnownExisting bool
}

func (c *Client) Get(k string, into io.Writer, opts GetOptions) (bool, error) {
	locs, err := c.GetClusterConfig().Crush(k)
	if err != nil {
		return false, err
	}

	errs := []error{}
	visited := make(map[string]struct{})
	for _, loc := range locs {
		server := loc[len(loc)-1]
		redirects := 0
		for {
			_, alreadyVisited := visited[server]
			visited[server] = struct{}{}
			if alreadyVisited {
				break
			}
			err := c.get(server, k, into, opts)
			if err != nil {
				if err, ok := err.(*ClientRequestError); ok {
					if err.StatusCode == 404 {
						continue
					}
					if err.StatusCode == 410 {
						return false, nil
					}
					if err.Redirect != "" && redirects < MAX_REDIRECTS {
						server = err.Redirect
						redirects += 1
						continue
					}
				}
				errs = append(errs, err)
				break
			}
			return true, nil
		}
	}
	switch len(errs) {
	case 0:
		return false, nil
	case 1:
		return false, errs[0]
	default:
		return false, &MultiError{Errors: errs}
	}
}

func (c *Client) delete(server string, k string, opts DeleteOptions) error {
	endpoint := fmt.Sprintf("%s/delete?key=%s", server, url.QueryEscape(k))
	resp, err := c.http.PostForm(endpoint, url.Values{})
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
	locs, err := c.GetClusterConfig().Crush(k)
	if err != nil {
		return err
	}

	// Delete at the primary server.
	server := locs[0][len(locs[0])-1]
	redirects := 0
	for {
		err := c.delete(server, k, opts)
		if err != nil {
			if err, ok := err.(*ClientRequestError); ok && err.Redirect != "" && redirects < MAX_REDIRECTS {
				server = err.Redirect
				redirects += 1
				continue
			}
			return err
		}
		return nil
	}
}

func (c *Client) iterBegin(server string, typ string) (string, error) {
	endpoint := fmt.Sprintf("%s/iter_begin?type=%s", server, url.QueryEscape(typ))
	resp, err := c.http.PostForm(endpoint, url.Values{})
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

func (c *Client) iterNext(server string, cursor string, iterOut interface{}) error {
	endpoint := fmt.Sprintf("%s/iter_next?it=%s", server, url.QueryEscape(cursor))
	resp, err := c.http.PostForm(endpoint, url.Values{})
	if err != nil {
		return err
	}
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

type RemoteObjectMetadata struct {
	Server    string
	Key       string
	Size      uint64
	Tombstone bool
	CreatedAt time.Time
}

type ListOptions struct {
}

func (c *Client) List(cb func(RemoteObjectMetadata) bool, opts ListOptions) error {
	cfg := c.GetClusterConfig()
	for _, node := range cfg.StorageHierarchy.StorageNodes {
		if node.IsDefunct() {
			continue
		}
		server := node.Location[len(node.Location)-1]
		cursor, err := c.iterBegin(server, "objects")
		if err != nil {
			return err
		}
		objects := []struct {
			Key                string
			Size               uint64
			Tombstone          bool
			CreatedAtUnixMicro uint64
		}{}
		for {
			err := c.iterNext(server, cursor, &objects)
			if err != nil {
				return err
			}
			if len(objects) == 0 {
				break
			}
			for _, o := range objects {
				cont := cb(RemoteObjectMetadata{
					Server:    server,
					Key:       o.Key,
					Size:      o.Size,
					Tombstone: o.Tombstone,
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
		cursor, err := c.iterBegin(server, "keys")
		if err != nil {
			return err
		}
		keys := []string{}
		for {
			err := c.iterNext(server, cursor, &keys)
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
