package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/client"
	"github.com/dustin/go-humanize"
)

func main() {

	cli.RegisterDefaultFlags()

	queryDefunct := flag.Bool("defunct", false, "Query defunct nodes.")
	showErrors := flag.Bool("show-errors", false, "Print errors to stderr.")

	flag.Parse()

	c := cli.MustOpenClient()
	defer c.Close()

	clusterConfig := c.GetClusterConfig()

	status := c.ClusterStatus(client.ClusterStatusOptions{
		QueryDefunct: *queryDefunct,
	})

	nodesRebalancing := uint64(0)

	nodeWithLeastFreeSpace := ""
	leastFreeSpace := uint64(0)

	nodeWithLeastFreeRAM := ""
	leastFreeRAM := uint64(0)

	nodeWithLongestLastScrub := ""
	longestLastScrub := time.Duration(0)

	nodeWithLongestLastFullScrub := ""
	longestLastFullScrub := time.Duration(0)

	nodeWithOldestLastScrub := ""
	oldestLastScrubUnixMicro := uint64(0)

	nodeWithOldestLastFullScrub := ""
	oldestLastFullScrubUnixMicro := uint64(0)

	nodeWithMostCorruptionErrors := ""
	mostCorruptionErrors := uint64(0)

	nodeWithMostIOErrors := ""
	mostIOErrors := uint64(0)

	nodeWithMostObjects := ""
	mostObjects := uint64(0)

	totalClusterUsedSpace := uint64(0)
	totalClusterFreeSpace := uint64(0)
	totalClusterObjects := uint64(0)

	for _, node := range status.Nodes {
		nodeInfo := status.NodeInfo[node]

		totalClusterUsedSpace += nodeInfo.UsedSpace
		totalClusterFreeSpace += nodeInfo.FreeSpace
		totalClusterObjects += nodeInfo.LastScrubObjects

		if nodeInfo.Rebalancing || nodeInfo.ConfigId != clusterConfig.ConfigId {
			nodesRebalancing += 1
		}

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

		if oldestLastScrubUnixMicro == 0 || nodeInfo.LastScrubUnixMicro < oldestLastScrubUnixMicro {
			nodeWithOldestLastScrub = node
			oldestLastScrubUnixMicro = nodeInfo.LastScrubUnixMicro
		}

		if oldestLastFullScrubUnixMicro == 0 || nodeInfo.LastFullScrubUnixMicro < oldestLastFullScrubUnixMicro {
			nodeWithOldestLastFullScrub = node
			oldestLastFullScrubUnixMicro = nodeInfo.LastFullScrubUnixMicro
		}

		if mostCorruptionErrors == 0 || nodeInfo.TotalScrubCorruptionErrorCount > mostCorruptionErrors {
			nodeWithMostCorruptionErrors = node
			mostCorruptionErrors = nodeInfo.TotalScrubCorruptionErrorCount
		}

		if mostIOErrors == 0 || nodeInfo.TotalScrubIOErrorCount > mostIOErrors {
			nodeWithMostIOErrors = node
			mostIOErrors = nodeInfo.TotalScrubIOErrorCount
		}

		if mostObjects == 0 || nodeInfo.LastScrubObjects > mostObjects {
			nodeWithMostObjects = node
			mostObjects = nodeInfo.LastScrubObjects
		}
	}

	if *showErrors {
		for _, err := range status.Errors {
			_, _ = fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		}
	}

	now := time.Now()
	_, _ = fmt.Printf("Notable nodes:\n")
	_, _ = fmt.Printf("  least free space: %q - %s\n", nodeWithLeastFreeSpace, humanize.IBytes(leastFreeSpace))
	_, _ = fmt.Printf("  least free ram: %q - %s\n", nodeWithLeastFreeRAM, humanize.IBytes(leastFreeRAM))
	_, _ = fmt.Printf("  most objects: %q - %d\n", nodeWithMostObjects, mostObjects)
	_, _ = fmt.Printf("  longest last scrub: %q - %s\n", nodeWithLongestLastScrub, longestLastScrub.Truncate(time.Second))
	_, _ = fmt.Printf("  longest last full scrub: %q - %s\n", nodeWithLongestLastFullScrub, longestLastFullScrub.Truncate(time.Second))
	_, _ = fmt.Printf("  oldest last scrub: %q - %s\n", nodeWithOldestLastScrub, now.Sub(time.UnixMicro(int64(oldestLastScrubUnixMicro))).Truncate(time.Second))
	_, _ = fmt.Printf("  oldest last full scrub: %q - %s\n", nodeWithOldestLastFullScrub, now.Sub(time.UnixMicro(int64(oldestLastFullScrubUnixMicro))).Truncate(time.Second))
	_, _ = fmt.Printf("  most corruption errors: %q - %d\n", nodeWithMostCorruptionErrors, mostCorruptionErrors)
	_, _ = fmt.Printf("  most io errors: %q - %d\n\n", nodeWithMostIOErrors, mostIOErrors)
	_, _ = fmt.Printf("Cluster Summary:\n")
	_, _ = fmt.Printf("  Cluster size: %d\n", len(status.Nodes))
	_, _ = fmt.Printf("  Rebalancing nodes: %d\n", nodesRebalancing)
	_, _ = fmt.Printf("  Unreachable nodes: %d\n", len(status.UnreachableNodes))
	_, _ = fmt.Printf("  Defunct nodes: %d\n", len(status.DefunctNodes))
	_, _ = fmt.Printf("  Total free space: %s (%s triple replicated)\n", humanize.IBytes(totalClusterFreeSpace), humanize.IBytes(totalClusterFreeSpace/3))
	_, _ = fmt.Printf("  Total used space: %s\n", humanize.IBytes(totalClusterUsedSpace))
	_, _ = fmt.Printf("  Total objects: %d (%d triple replicated)\n", totalClusterObjects, totalClusterObjects/3)

}
