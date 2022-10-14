package clusterconfig

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andrewchambers/crushstore/crush"
	"github.com/google/shlex"
	"gopkg.in/yaml.v3"
	"lukechampine.com/blake3"
)

type ClusterConfig struct {
	ConfigId         string
	ConfigBytes      []byte
	ClusterSecret    string
	PlacementRules   []crush.CrushSelection
	StorageHierarchy *crush.StorageHierarchy
}

func (cfg *ClusterConfig) Crush(k string) ([]crush.Location, error) {
	return cfg.StorageHierarchy.Crush(k, cfg.PlacementRules)
}

func ParseClusterConfig(configYamlBytes []byte) (*ClusterConfig, error) {

	configHash := blake3.Sum256(configYamlBytes)

	newConfig := &ClusterConfig{
		ConfigId:    hex.EncodeToString(configHash[:16]),
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

type ConfigWatcher interface {
	GetCurrentConfig() *ClusterConfig
	ReloadConfig() (*ClusterConfig, error)
	OnConfigChange(func(*ClusterConfig, error))
	Stop()
}

type ConfigFileWatcher struct {
	configPath       string
	currentConfig    atomic.Value
	reloadLock       sync.Mutex
	lastUpdate       time.Time
	onChangeCallback func(*ClusterConfig, error)
	cancelWorker     func()
	workerWg         sync.WaitGroup
}

func NewConfigFileWatcher(configPath string) (*ConfigFileWatcher, error) {

	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	config, err := ParseClusterConfig(configBytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", configPath, err)
	}

	watcher := &ConfigFileWatcher{
		configPath:       configPath,
		lastUpdate:       time.Now(),
		onChangeCallback: func(cfg *ClusterConfig, err error) {},
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	watcher.cancelWorker = cancelWorker
	watcher.currentConfig.Store(config)
	watcher.workerWg.Add(1)
	go func() {
		defer watcher.workerWg.Done()

		checkTicker := time.NewTicker(30 * time.Second)
		defer checkTicker.Stop()

		for {
			select {
			case <-workerCtx.Done():
				return
			case <-checkTicker.C:
				_, _ = watcher.ReloadConfig()
			}
		}
	}()

	return watcher, nil
}

func (w *ConfigFileWatcher) GetCurrentConfig() *ClusterConfig {
	cfg, _ := w.currentConfig.Load().(*ClusterConfig)
	return cfg
}

func (w *ConfigFileWatcher) OnConfigChange(cb func(cfg *ClusterConfig, err error)) {
	w.reloadLock.Lock()
	defer w.reloadLock.Unlock()
	w.onChangeCallback = cb
}

func (w *ConfigFileWatcher) ReloadConfig() (*ClusterConfig, error) {
	w.reloadLock.Lock()
	defer w.reloadLock.Unlock()

	stat, err := os.Stat(w.configPath)
	if err != nil {
		w.onChangeCallback(nil, err)
		return nil, err
	}

	currentConfig := w.GetCurrentConfig()

	if !stat.ModTime().After(w.lastUpdate) {
		return currentConfig, nil
	}

	configBytes, err := os.ReadFile(w.configPath)
	if err != nil {
		w.onChangeCallback(nil, err)
		return nil, err
	}

	if bytes.Equal(currentConfig.ConfigBytes, configBytes) {
		return currentConfig, nil
	}

	newConfig, err := ParseClusterConfig(configBytes)
	if err != nil {
		err = fmt.Errorf("error parsing %s: %w", w.configPath, err)
		w.onChangeCallback(nil, err)
		return nil, err
	}

	w.currentConfig.Store(newConfig)
	w.onChangeCallback(newConfig, nil)
	w.lastUpdate = stat.ModTime()
	return newConfig, nil
}

func (w *ConfigFileWatcher) Stop() {
	w.cancelWorker()
	w.workerWg.Wait()
}
