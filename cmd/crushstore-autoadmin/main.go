package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/client"
	"github.com/google/shlex"
	"gopkg.in/yaml.v3"
)

func replaceFile(path string, contents []byte) error {
	var mode fs.FileMode
	stat, err := os.Stat(path)
	if err == nil {
		mode = stat.Mode()
	} else if os.IsNotExist(err) {
		mode = 0644
	} else {
		return err
	}
	tmpPath := path + ".tmp"
	err = os.WriteFile(tmpPath, contents, mode)
	if err != nil {
		return err
	}
	err = os.Rename(tmpPath, path)
	if err != nil {
		return err
	}
	return err
}

type AutoadminState struct {
	UnreachableSince map[string]time.Time
}

func patchConfig(configBytes []byte, defunct map[string]struct{}, weights map[string]uint64) ([]byte, error) {
	var rawConfig yaml.Node
	err := yaml.Unmarshal(configBytes, &rawConfig)
	if err != nil {
		return nil, err
	}
	configKvs := rawConfig.Content[0]
	var storageNodes *yaml.Node
	for i := 0; i < len(configKvs.Content); i += 2 {
		if configKvs.Content[i].Value == "storage-nodes" {
			storageNodes = configKvs.Content[i+1]
			break
		}
	}
	re := regexp.MustCompile(`([^ ]+) ([^ ]+) (.*)`)
	for _, storageNode := range storageNodes.Content {
		storageNodeConfigParts, _ := shlex.Split(storageNode.Value)
		matches := re.FindStringSubmatchIndex(storageNode.Value)
		if matches == nil {
			return nil, fmt.Errorf("unable to patch storage node %q, it didn't match the required regex", storageNode.Value)
		}
		node := storageNodeConfigParts[len(storageNodeConfigParts)-1]
		weight := "$1"
		if newWeight, ok := weights[node]; ok {
			weight = fmt.Sprintf("%d", newWeight)
		}
		state := "healthy"
		if _, isDefunct := defunct[node]; isDefunct {
			state = "defunct"
		}
		template := weight + " " + state + " $3"
		patchedStorageNodeConfig := string(re.ExpandString(nil, template, storageNode.Value, matches))
		storageNode.Value = patchedStorageNodeConfig
	}
	return yaml.Marshal(&rawConfig)
}

type UpdateClusterConfigOptions struct {
	StateFile          string
	UpdateWeights      bool
	DryRun             bool
	UnreachableTimeout time.Duration
}

func UpdateClusterConfig(configPath string, opts UpdateClusterConfigOptions) error {
	// XXX we don't want dynamic config reloading.
	c, err := client.New(configPath, client.ClientOptions{})
	if err != nil {
		return err
	}
	defer c.Close()

	var state AutoadminState

	if opts.StateFile != "" {
		stateBytes, err := os.ReadFile(opts.StateFile)
		if err == nil {
			err = json.Unmarshal(stateBytes, &state)
			if err != nil {
				return fmt.Errorf("unable to load state file %q: %w", opts.StateFile, err)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("unable to read state file %q: %w", opts.StateFile, err)
		}
	}

	if state.UnreachableSince == nil {
		state.UnreachableSince = make(map[string]time.Time)
	}

	clusterConfig := c.GetClusterConfig()
	clusterStatus := c.ClusterStatus(client.ClusterStatusOptions{
		QueryDefunct: true,
	})

	if len(clusterStatus.UnreachableNodes) >= len(clusterStatus.Nodes)/2+len(clusterStatus.Nodes)%2 {
		return fmt.Errorf(
			"refusing to update config when less than half the cluster is reachable (%d/%d)",
			len(clusterStatus.Nodes)-len(clusterStatus.UnreachableNodes), len(clusterStatus.Nodes),
		)
	}

	weights := make(map[string]uint64)
	defunct := make(map[string]struct{})

	for _, node := range clusterStatus.Nodes {
		nodeInfo, ok := clusterStatus.NodeInfo[node]
		if !ok {
			log.Printf("%q is unreachable", node)
			since, ok := state.UnreachableSince[node]
			if ok {
				if time.Now().Sub(since) > opts.UnreachableTimeout {
					log.Printf("%q is defunct because it is has been unreachable longer than %s", node, opts.UnreachableTimeout)
					defunct[node] = struct{}{}
				}
			} else {
				state.UnreachableSince[node] = time.Now()
			}
			continue
		}

		delete(state.UnreachableSince, node)

		if opts.UpdateWeights {
			GiBTotal := (nodeInfo.FreeSpace + nodeInfo.UsedSpace) / (1024 * 1024 * 1024)
			// XXX if a node is taking more than its fair share of space
			// we should apply some sort of overload adjustment.
			// We need to work out how to apply this without causing oscillation in the cluster.
			weights[node] = GiBTotal
		}

		if nodeInfo.Rebalancing || nodeInfo.ConfigId != clusterConfig.ConfigId {
			// Do not jitter the cluster config more than rebalancing is able to cope.
			//
			// This keeps a few corner cases simple:
			//
			// - Objects are hard to locate if the config changes faster than rebalancing.
			// - It limits the chance of strange interactions when bouncing back and forth between configs.
			return fmt.Errorf("refusing to change cluster because %q has not yet rebalanced", node)
		}

		if nodeInfo.TotalScrubCorruptionErrorCount != 0 {
			// This node has corrupted objects in the past,
			// it is now defunct until there is manual intervention
			// to clear the error counters.
			defunct[node] = struct{}{}
			log.Printf("%q is now defunct because it has corruption errors", node)
			continue
		}

		if nodeInfo.TotalScrubIOErrorCount != 0 {
			// On filesystems like btrfs and zfs io errors are
			// a sign of corruption - this also requires manual intervention.
			defunct[node] = struct{}{}
			log.Printf("%q is now defunct because it has io errors", node)
			continue
		}
	}

	for _, previouslyDefunct := range clusterStatus.DefunctNodes {
		_, isDefunct := defunct[previouslyDefunct]
		if !isDefunct {
			log.Printf("%q is now healthy", previouslyDefunct)
		}
	}

	if opts.StateFile != "" && !opts.DryRun {
		stateBytes, err := json.Marshal(state)
		if err != nil {
			return err
		}
		err = replaceFile(opts.StateFile, stateBytes)
		if err != nil {
			return err
		}
	}

	newConfigBytes, err := patchConfig(clusterConfig.ConfigBytes, defunct, weights)
	if err != nil {
		return err
	}

	if !opts.DryRun {
		log.Printf("updating %q", configPath)
		err = replaceFile(configPath, newConfigBytes)
		if err != nil {
			return err
		}
	} else {
		log.Printf("would update %q to contain:\n%s", configPath, string(newConfigBytes))
	}

	return nil
}

func main() {
	cli.RegisterDefaultFlags()
	dryRun := flag.Bool("dry-run", false, "Do a try run without changing any config.")
	stateFile := flag.String("state", "", "Store state in this file (enables more sophisticated checks).")
	updateWeights := flag.Bool("update-weights", false, "Update the cluster config weights based on disk usage.")
	unreachableTimeout := flag.Duration("unreachable-timeout", 60*time.Minute, "If a node has been unreachable for this long, mark it as defunct (requires -state).")
	flag.Parse()
	err := UpdateClusterConfig(cli.ClusterConfigFile, UpdateClusterConfigOptions{
		DryRun:             *dryRun,
		StateFile:          *stateFile,
		UpdateWeights:      *updateWeights,
		UnreachableTimeout: *unreachableTimeout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to update config: %s\n", err)
		os.Exit(1)
	}
}
