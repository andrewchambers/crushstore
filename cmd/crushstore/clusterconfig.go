package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/andrewchambers/crushstore/crush"
	"github.com/google/shlex"
	"gopkg.in/yaml.v3"
)

var _clusterConfig atomic.Value

type ClusterConfig struct {
	ConfigBytes      []byte
	ClusterSecret    string
	PlacementRules   []crush.CrushSelection
	StorageHierarchy *crush.StorageHierarchy
}

func (cfg *ClusterConfig) Crush(k string) ([]crush.Location, error) {
	return cfg.StorageHierarchy.Crush(k, cfg.PlacementRules)
}

func SetClusterConfig(cfg *ClusterConfig) {
	if !cfg.StorageHierarchy.ContainsStorageNodeAtLocation(ThisLocation) {
		log.Printf("WARNING - config storage hierarchy does not contain the current node at %s.", ThisLocation)
	}
	_clusterConfig.Store(cfg)
	TriggerScrub()
}

func GetClusterConfig() *ClusterConfig {
	config, _ := _clusterConfig.Load().(*ClusterConfig)
	return config
}

func ParseClusterConfig(configYamlBytes []byte) (*ClusterConfig, error) {

	newConfig := &ClusterConfig{
		ConfigBytes: configYamlBytes,
	}

	rawConfig := struct {
		ClusterSecret  string   `yaml:"cluster-secret"`
		StorageSchema  string   `yaml:"storage-schema"`
		PlacementRules []string `yaml:"placement-rules"`
		StorageNodes   []string `yaml:"storage-nodes"`
	}{}

	decoder := yaml.NewDecoder(bytes.NewReader(configYamlBytes))
	decoder.KnownFields(true)
	err := decoder.Decode(&rawConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to load yaml config: %w", err)
	}

	newConfig.ClusterSecret = rawConfig.ClusterSecret

	newConfig.StorageHierarchy, err = crush.NewStorageHierarchyFromSchema(rawConfig.StorageSchema)
	if err != nil {
		return nil, fmt.Errorf("unable parse storage-schema %q: %w", rawConfig.StorageSchema, err)
	}

	parseNodeInfo := func(s string) (*crush.StorageNodeInfo, error) {
		parts, err := shlex.Split(s)
		if err != nil {
			return nil, fmt.Errorf("unable to split storage node spec %q into components: %w", s, err)
		}
		if len(parts) < 3 {
			return nil, fmt.Errorf("storage node needs at least 3 components")
		}

		weight, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing weight %q: %w", parts[0], err)
		}

		var defunct bool
		switch parts[1] {
		case "healthy":
			defunct = false
		case "defunct":
			defunct = true
		default:
			return nil, fmt.Errorf("unknown node status %q, expected 'healthy' or 'defunct'", parts[1])
		}

		return &crush.StorageNodeInfo{
			Weight:   weight,
			Defunct:  defunct,
			Location: crush.Location(parts[2:]),
		}, nil
	}

	// TODO rename CrushSelection to PlacementRule
	parsePlacementRule := func(s string) (crush.CrushSelection, error) {
		parts, err := shlex.Split(s)
		if err != nil {
			return crush.CrushSelection{}, fmt.Errorf("unable to split placement rule %q into components: %w", s, err)
		}
		if len(parts) < 1 {
			return crush.CrushSelection{}, fmt.Errorf("unexpected empty placement rule")
		}
		switch parts[0] {
		case "select":
			if len(parts) != 3 {
				return crush.CrushSelection{}, fmt.Errorf("select placement rules require 2 arguments")
			}
			typeName := parts[1]
			count, err := strconv.Atoi(parts[2])
			if err != nil {
				return crush.CrushSelection{}, fmt.Errorf("unable to parse select count %q: %w", parts[2], err)
			}
			return crush.CrushSelection{
				Type:  typeName,
				Count: count,
			}, nil
		default:
			return crush.CrushSelection{}, fmt.Errorf("unexpected placement operator %q", parts[0])
		}
	}

	for _, placementRuleString := range rawConfig.PlacementRules {
		placementRule, err := parsePlacementRule(placementRuleString)
		if err != nil {
			return nil, fmt.Errorf("unable parse placement rule %q: %w", placementRuleString, err)
		}
		newConfig.PlacementRules = append(newConfig.PlacementRules, placementRule)
	}

	for _, storageNodeString := range rawConfig.StorageNodes {
		nodeInfo, err := parseNodeInfo(storageNodeString)
		if err != nil {
			return nil, fmt.Errorf("unable parse %q storage-schema: %w", storageNodeString, err)
		}
		err = newConfig.StorageHierarchy.AddStorageNode(nodeInfo)
		if err != nil {
			return nil, fmt.Errorf("unable add %q to storage hierarchy: %w", storageNodeString, err)
		}
	}
	newConfig.StorageHierarchy.Finish()

	return newConfig, nil
}

func ReloadClusterConfigFromFile(configPath string) error {
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	currentConfig := GetClusterConfig()
	if currentConfig != nil {
		if bytes.Equal(configBytes, currentConfig.ConfigBytes) {
			return nil
		}
	}
	newConfig, err := ParseClusterConfig(configBytes)
	if err != nil {
		return err
	}
	SetClusterConfig(newConfig)
	return nil
}

func WatchClusterConfigForever(configPath string) {
	lastUpdate := time.Now()
	for {
		stat, err := os.Stat(configPath)
		if err != nil {
			log.Printf("unable to stat config: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if stat.ModTime().After(lastUpdate) {
			log.Printf("detected config change, reloading.")
			err = ReloadClusterConfigFromFile(configPath)
			if err != nil {
				log.Printf("error reloading config: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			lastUpdate = stat.ModTime()
		}

		// Check the config on fixed unix time boundaries, this
		// means our cluster is more likely to reload their configs
		// in sync when polling a network config.
		const RELOAD_BOUNDARY = 60
		nowUnix := time.Now().Unix()
		delaySecs := int64(RELOAD_BOUNDARY / 2)
		// XXX loop is dumb (but works).
		for {
			if (nowUnix+delaySecs)%RELOAD_BOUNDARY == 0 {
				break
			}
			delaySecs += 1
		}
		time.Sleep(time.Duration(delaySecs) * time.Second)
	}
}
