package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/andrewchambers/crushstore/client"
	"github.com/dustin/go-humanize"
)

func main() {
	clusterConfigFile := flag.String("cluster-config", "./crushstore-cluster.conf", "Path to cluster config.")
	showErrors := flag.Bool("show-errors", false, "Print errors to stderr.")

	flag.Parse()

	c, err := client.New(*clusterConfigFile, client.ClientOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error creating client: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	status := c.ClusterStatus(client.ClusterStatusOptions{})

	nodeWithLeastFreeSpace := ""
	leastFreeSpace := uint64(0)

	nodeWithLeastFreeRAM := ""
	leastFreeRAM := uint64(0)

	nodeWithLongestLastScrub := ""
	longestLastScrub := time.Duration(0)

	nodeWithLongestLastFullScrub := ""
	longestLastFullScrub := time.Duration(0)

	nodeWithMostScrubErrors := ""
	mostScrubErrors := uint64(0)

	nodeWithMostObjects := ""
	mostObjects := uint64(0)

	totalClusterUsedSpace := uint64(0)
	totalClusterFreeSpace := uint64(0)
	totalClusterObjects := uint64(0)

	for _, node := range status.Nodes {
		nodeInfo := status.NodeInfo[node]

		totalClusterUsedSpace += nodeInfo.UsedSpace
		totalClusterFreeSpace += nodeInfo.FreeSpace
		totalClusterObjects += nodeInfo.ObjectCount

		if leastFreeSpace == 0 || nodeInfo.FreeSpace < leastFreeSpace {
			nodeWithLeastFreeSpace = node
			leastFreeSpace = nodeInfo.FreeSpace
		}

		if leastFreeRAM == 0 || nodeInfo.FreeRAM < leastFreeRAM {
			nodeWithLeastFreeRAM = node
			leastFreeRAM = nodeInfo.FreeRAM
		}

		if longestLastScrub == 0 || nodeInfo.LastScrubDuration > longestLastScrub {
			nodeWithLongestLastScrub = node
			longestLastScrub = nodeInfo.LastScrubDuration
		}

		if longestLastFullScrub == 0 || nodeInfo.LastFullScrubDuration > longestLastFullScrub {
			nodeWithLongestLastFullScrub = node
			longestLastFullScrub = nodeInfo.LastFullScrubDuration
		}

		if mostScrubErrors == 0 || nodeInfo.LastScrubErrorCount > mostScrubErrors {
			nodeWithMostScrubErrors = node
			mostScrubErrors = nodeInfo.LastScrubErrorCount
		}

		if mostObjects == 0 || nodeInfo.ObjectCount > mostObjects {
			nodeWithMostObjects = node
			mostObjects = nodeInfo.ObjectCount
		}
	}

	if *showErrors {
		for _, err := range status.Errors {
			_, _ = fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		}
	}
	_, _ = fmt.Printf("Node with least free space: %q - %s\n", nodeWithLeastFreeSpace, humanize.IBytes(leastFreeSpace))
	_, _ = fmt.Printf("Node with least free ram: %q - %s\n", nodeWithLeastFreeRAM, humanize.IBytes(leastFreeRAM))
	_, _ = fmt.Printf("Node with most objects: %q - %d\n", nodeWithMostObjects, mostObjects)
	_, _ = fmt.Printf("Node with longest last scrub: %q - %s\n", nodeWithLongestLastScrub, longestLastScrub)
	_, _ = fmt.Printf("Node with longest last full scrub: %q - %s\n", nodeWithLongestLastFullScrub, longestLastFullScrub)
	_, _ = fmt.Printf("Node with most scrub errors: %q - %d\n", nodeWithMostScrubErrors, mostScrubErrors)
	_, _ = fmt.Printf("Total free space: %s\n", humanize.IBytes(totalClusterFreeSpace))
	_, _ = fmt.Printf("Total used space: %s\n", humanize.IBytes(totalClusterUsedSpace))
	_, _ = fmt.Printf("Total objects: %d\n", totalClusterObjects)
	_, _ = fmt.Printf("Total nodes: %d\n", len(status.Nodes))
	_, _ = fmt.Printf("Unreachable nodes: %d\n", len(status.Unreachable))
}
