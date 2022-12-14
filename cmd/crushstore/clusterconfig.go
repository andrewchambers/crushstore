package main

import (
	"log"
	"sync/atomic"

	"github.com/andrewchambers/crushstore/clusterconfig"
)

var (
	TheConfigWatcher     clusterconfig.ConfigWatcher
	_configChangeCounter uint64
)

func SetConfigWatcher(w clusterconfig.ConfigWatcher) {
	w.OnConfigChange(func(cfg *clusterconfig.ClusterConfig, err error) {
		log.Printf("config change detected")
		if err != nil {
			log.Printf("WARNING - config change failed: %s", err)
			return
		}
		if !cfg.StorageHierarchy.ContainsStorageNodeAtLocation(ThisLocation) {
			log.Printf("WARNING - config storage hierarchy does not contain the current node at %s", ThisLocation)
		}
		atomic.AddUint64(&_configChangeCounter, 1)
		TriggerScrub(TriggerScrubOptions{FullScrub: false})
	})
	TheConfigWatcher = w
}

func GetClusterConfig() *clusterconfig.ClusterConfig {
	return TheConfigWatcher.GetCurrentConfig()
}

func ReloadClusterConfig() (*clusterconfig.ClusterConfig, error) {
	return TheConfigWatcher.ReloadConfig()
}
