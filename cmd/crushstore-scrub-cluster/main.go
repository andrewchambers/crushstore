package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/client"
)

func main() {
	cli.RegisterDefaultFlags()
	full := flag.Bool("full", false, "Force a full scrub")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "progress poll interval")
	flag.Parse()
	c := cli.MustOpenClient()
	defer c.Close()
	lastProgress := client.ScrubClusterProgress{}
	err := c.ScrubCluster(func(progress client.ScrubClusterProgress) {
		_, _ = fmt.Printf(
			"scrubbed %d objects (%.2f%% of last scrub), %d nodes remaining\n",
			progress.ScrubbedObjects,
			progress.ApproximatePercentComplete,
			progress.NodesRemaining,
		)
		lastProgress = progress
	}, client.ScrubClusterOptions{
		FullScrub:    *full,
		PollInterval: *pollInterval,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable to scrub cluster: %s\n", err)
		os.Exit(1)
	}

	_, _ = fmt.Printf("scrub had %d corruption errors\n", lastProgress.CorruptionErrors)
	_, _ = fmt.Printf("scrub had %d replication errors\n", lastProgress.ReplicationErrors)
	_, _ = fmt.Printf("scrub had %d other errors\n", lastProgress.OtherErrors)

	errorCount := uint64(0)
	errorCount += lastProgress.CorruptionErrors
	errorCount += lastProgress.ReplicationErrors
	errorCount += lastProgress.OtherErrors
	if errorCount != 0 {
		os.Exit(1)
	}

}
