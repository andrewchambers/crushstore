package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/andrewchambers/crushstore/clusterconfig"
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

func (c *Client) Put(key string, data io.Reader, opts PutOptions) error {
	return errors.New("TODO")
}

type GetOptions struct {
}

func (c *Client) Get(key string, info io.Writer, opts GetOptions) error {
	return errors.New("TODO")
}

type DeleteOptions struct {
}

func (c *Client) Delete(key string, opts DeleteOptions) error {
	return errors.New("TODO")
}

/*
type ListOptions struct{

}

func (c *Client) List(onKey func (string) error, opts ListOptions) error {
	return errors.New("TODO")
}
*/
