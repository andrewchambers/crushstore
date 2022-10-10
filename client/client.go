package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	config              atomic.Value
	configWorkerContext context.Context
	cancelConfigWorker  func()
	workerWg            sync.WaitGroup
}

type ClientOptions struct {
	OnConfigError func(err error)
}

func New(configPath string, opts ClientOptions) (*Client, error) {

	if opts.OnConfigError == nil {
		opts.OnConfigError = func(err error) {
			log.Printf("error reloading config: %s", err)
		}
	}

	c := &Client{}

	config, err := clusterconfig.LoadClusterConfigFromFile(configPath)
	if err != nil {
		return nil, err
	}
	c.config.Store(config)

	c.configWorkerContext, c.cancelConfigWorker = context.WithCancel(context.Background())

	c.workerWg.Add(1)
	go func() {
		defer c.workerWg.Done()
		clusterconfig.WatchClusterConfig(c.configWorkerContext, configPath, func(newConfig *clusterconfig.ClusterConfig, err error) {
			if err != nil {
				opts.OnConfigError(err)
				return
			}
			oldConfig := c.GetClusterConfig()
			if !bytes.Equal(oldConfig.ConfigBytes, newConfig.ConfigBytes) {
				c.config.Store(newConfig)
			}
		})
	}()

	return c, nil
}

func (c *Client) Close() error {
	c.cancelConfigWorker()
	c.workerWg.Wait()
	return nil
}

func (c *Client) GetClusterConfig() *clusterconfig.ClusterConfig {
	config, _ := c.config.Load().(*clusterconfig.ClusterConfig)
	return config
}

type PutOptions struct {
}

func (c *Client) Put(k string, data io.ReadSeeker, opts PutOptions) error {

	locs, err := c.GetClusterConfig().Crush(k)
	if err != nil {
		return err
	}

	errs := []error{}

	for i, loc := range locs {

		if i != 0 {
			_, err := data.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}
		}

		server := loc[len(loc)-1]

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
		resp, err := http.Post(endpoint, mpw.FormDataContentType(), r)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to read response from %s: %s", endpoint, err))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			errs = append(errs, fmt.Errorf("upload failed: %s, body: %q", resp.Status, body))
			continue
		}

		uploadErr := errg.Wait()
		if err != nil {
			errs = append(errs, uploadErr)
		}

		return nil
	}

	// XXX some sort of multi error?
	return errs[0]

}

type GetOptions struct {
}

func (c *Client) Get(k string, info io.Writer, opts GetOptions) error {
	return errors.New("TODO")
}

type DeleteOptions struct {
}

func (c *Client) Delete(k string, opts DeleteOptions) error {

	locs, err := c.GetClusterConfig().Crush(k)
	if err != nil {
		return err
	}

	errs := []error{}

	for _, loc := range locs {
		server := loc[len(loc)-1]
		endpoint := fmt.Sprintf("%s/delete", server)
		resp, err := http.PostForm("https://httpbin.org/post", url.Values{
			"key": {k},
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to read response from %s: %s", endpoint, err))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			errs = append(errs, fmt.Errorf("delete failed: %s, body: %q", resp.Status, body))
			continue
		}

		return nil
	}

	// XXX some sort of multi error?
	return errs[0]
}

/*
type ListOptions struct{

}

func (c *Client) List(onKey func (string) error, opts ListOptions) error {
	return errors.New("TODO")
}
*/
