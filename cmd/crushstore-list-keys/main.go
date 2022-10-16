package main

import (
	"encoding/json"
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

	err := c.ListKeys(func(rk client.RemoteKey) bool {
		buf, err := json.Marshal(&rk)
		if err != nil {
			panic(err)
		}
		buf = append(buf, '\n')
		_, err = os.Stdout.Write(buf)
		if err != nil {
			return false
		}
		return true
	}, client.ListKeysOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
