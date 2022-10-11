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

	for _, arg := range flag.Args() {
		err = c.Delete(arg, client.DeleteOptions{})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}

}
