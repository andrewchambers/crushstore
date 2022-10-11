package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/crushstore/client"
)

func main() {
	clusterConfigFile := flag.String("cluster-config", "./crushstore-cluster.conf", "Path to cluster config.")

	flag.Parse()

	c, err := client.New(*clusterConfigFile, client.ClientOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error creating client: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	args := flag.Args()

	if len(args) != 1 {
		_, _ = fmt.Fprintf(os.Stderr, "expected a key\n")
		os.Exit(1)
	}

	ok, err := c.Get(args[0], os.Stdout, client.GetOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if !ok {
		os.Exit(2)
	}
}
