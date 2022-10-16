package cli

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/crushstore/client"
)

var ClusterConfigFile string

func RegisterClusterConfigFlag() {
	defaultClusterConfigFile := os.Getenv("CRUSHSTORE_CLUSTER_CONFIG")
	if defaultClusterConfigFile == "" {
		defaultClusterConfigFile = "./crushstore-cluster.conf"
		_, err := os.Stat(defaultClusterConfigFile)
		if err != nil {
			defaultClusterConfigFile = "/etc/crushstore/crushstore-cluster.conf"
		}
	}
	flag.StringVar(
		&ClusterConfigFile,
		"cluster-config",
		defaultClusterConfigFile,
		"CrushStore cluster config, defaults to CRUSHSTORE_CLUSTER_CONFIG if set, ./crushstore-cluster.conf if present, otherwise /etc/crushstore/crushstore-cluster.conf",
	)
}

func RegisterDefaultFlags() {
	RegisterClusterConfigFlag()
}

func MustOpenClient() *client.Client {
	c, err := client.New(ClusterConfigFile, client.ClientOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create crushstore client: %s\n", err)
		os.Exit(1)
	}
	return c
}
