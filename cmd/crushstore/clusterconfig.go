package main

import (
	"bytes"
	"context"
	"log"
	"sync/atomic"

	"github.com/andrewchambers/crushstore/clusterconfig"
)

var _clusterConfig atomic.Value

func SetClusterConfig(cfg *clusterconfig.ClusterConfig) {
	if !cfg.StorageHierarchy.ContainsStorageNodeAtLocation(ThisLocation) {
		log.Printf("WARNING - config storage hierarchy does not contain the current node at %s.", ThisLocation)
	}
	_clusterConfig.Store(cfg)
	TriggerScrub()
}

func GetClusterConfig() *clusterconfig.ClusterConfig {
	config, _ := _clusterConfig.Load().(*clusterconfig.ClusterConfig)
	return config
}

func LoadClusterConfigFromFile(configPath string) error {
	cfg, err := clusterconfig.LoadClusterConfigFromFile(configPath)
	if err != nil {
		return err
	}
	SetClusterConfig(cfg)
	return nil
}

func WatchClusterConfigForever(configPath string) {
	clusterconfig.WatchClusterConfig(context.Background(), configPath, func(newConfig *clusterconfig.ClusterConfig, err error) {
		if err != nil {
			log.Printf("error reloading config: %s", err)
			return
		}
		log.Printf("detected config change, reloading")
		oldConfig := GetClusterConfig()
		if !bytes.Equal(oldConfig.ConfigBytes, newConfig.ConfigBytes) {
			SetClusterConfig(newConfig)
		}
	})
}
