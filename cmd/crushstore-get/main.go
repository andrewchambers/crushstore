package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/andrewchambers/crushstore/client"
	"golang.org/x/sys/unix"
)

func main() {

	startOffset := flag.Uint64("stat-offset", 0, "Start reading object at this offset.")
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

	obj, ok, err := c.Get(args[0], client.GetOptions{StartOffset: *startOffset})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	if !ok {
		os.Exit(2)
	}
	defer obj.Close()

	_, err = io.Copy(os.Stdout, obj)
	if err != nil && !errors.Is(err, unix.EPIPE) {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
