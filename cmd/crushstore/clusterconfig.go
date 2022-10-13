package main

import (
	"log"

	"github.com/andrewchambers/crushstore/clusterconfig"
)

var TheConfigWatcher clusterconfig.ConfigWatcher

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
		TriggerScrub()
	})
	TheConfigWatcher = w
}

func GetClusterConfig() *clusterconfig.ClusterConfig {
	return TheConfigWatcher.GetCurrentConfig()
}
