package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/crushstore/client"
)

func main() {
	deleted := flag.Bool("deleted", false, "List deleted objects.")
	clusterConfigFile := flag.String("cluster-config", "./crushstore-cluster.conf", "Path to cluster config.")

	flag.Parse()

	c, err := client.New(*clusterConfigFile, client.ClientOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error creating client: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.List(func(md client.RemoteObject) bool {
		buf, err := json.Marshal(&md)
		if err != nil {
			panic(err)
		}
		buf = append(buf, '\n')
		_, err = os.Stdout.Write(buf)
		if err != nil {
			return false
		}
		return true
	}, client.ListOptions{Deleted: *deleted})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
