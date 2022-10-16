package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/client"
)

func main() {

	cli.RegisterDefaultFlags()

	flag.Parse()

	c := cli.MustOpenClient()
	defer c.Close()

	for _, arg := range flag.Args() {
		err := c.Delete(arg, client.DeleteOptions{})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}

}
