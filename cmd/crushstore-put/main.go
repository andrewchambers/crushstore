package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/client"
)

func main() {

	cli.RegisterDefaultFlags()
	replicas := flag.Uint("replicas", 0, "The initial number of remote replicas (0 means full replication).")

	flag.Parse()

	c := cli.MustOpenClient()
	defer c.Close()

	args := flag.Args()

	if len(args) != 2 {
		_, _ = fmt.Fprintf(os.Stderr, "expected a key and an input file\n")
		os.Exit(1)
	}

	var f *os.File

	if args[1] == "-" {
		tmpF, err := os.CreateTemp("", "")
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "unable to create temporary file: %s\n", err)
			os.Exit(1)
		}
		defer os.Remove(tmpF.Name())
		_, err = io.Copy(tmpF, os.Stdin)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "io error: %s\n", err)
			os.Exit(1)
		}
		_, err = tmpF.Seek(0, io.SeekStart)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "io error: %s\n", err)
			os.Exit(1)
		}
		f = tmpF
	} else {
		f, err := os.Open(args[1])
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
		defer f.Close()
	}

	err := c.Put(args[0], f, client.PutOptions{
		Replicas: *replicas,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
