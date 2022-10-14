package main

import (
	"encoding/json"
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

	header, ok, err := c.Head(args[0], client.HeadOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	if !ok {
		os.Exit(2)
	}

	bytes, err := json.Marshal(&header)
	if err != nil {
		panic(err)
	}
	bytes = append(bytes, '\n')
	_, _ = os.Stdout.Write(bytes)
}
