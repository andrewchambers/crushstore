package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/andrewchambers/crushstore/cli"
	"github.com/andrewchambers/crushstore/clusterconfig"
	"github.com/andrewchambers/crushstore/crush"
	"github.com/google/shlex"
	"golang.org/x/sys/unix"
)

var ThisLocation crush.Location

func main() {

	cli.RegisterDefaultFlags()
	flag.DurationVar(&ScrubTempFileExpiry, "scrub-tempfile-expiry", ScrubTempFileExpiry, "The maximum lifetime of a temporary object.")
	flag.DurationVar(&ObjectTombstoneExpiry, "tombstone-expiry", ObjectTombstoneExpiry, "Time taken for object tombstones to be removed from the system.")
	flag.DurationVar(&ScrubInterval, "scrub-interval", ScrubInterval, "Time interval between metadata scrubs.")
	flag.DurationVar(&FullScrubInterval, "full-scrub-interval", FullScrubInterval, "Time between full data scrubs.")
	flag.IntVar(&ScrubParallelism, "scrub-parallelism", ScrubParallelism, "Number of data objects to scrub in parallel.")

	listenAddress := flag.String("listen-address", "", "Address to listen on.")
	location := flag.String("location", "", "Storage location specification, defaults to http://${listen-address}.")
	dataDir := flag.String("data-dir", "", "Directory to store objects under.")

	flag.Parse()

	if ScrubParallelism <= 0 {
		ScrubParallelism = 1
	}

	if *listenAddress == "" {
		log.Fatalf("-listen-address not specified.")
	}

	if *location == "" {
		*location = fmt.Sprintf("http://%s", *listenAddress)
	}
	parsedLocation, err := shlex.Split(*location)
	if err != nil {
		log.Fatalf("error parsing -location: %s", err)
	}

	if len(parsedLocation) == 0 {
		log.Fatalf("-location must have at least one component")
	}

	ThisLocation = crush.Location(parsedLocation)

	rLimit := unix.Rlimit{}
	err = unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Fatalf("error getting open file rlimit: %s", err)
	}
	rLimit.Cur = rLimit.Max
	err = unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("error setting open file rlimit", err)
	}

	if *dataDir == "" {
		log.Fatalf("-data-dir not specified.")
	}
	err = OpenObjectDir(ThisLocation, *dataDir)
	if err != nil {
		log.Fatalf("error preparing -data-dir: %s", err)
	}

	configWatcher, err := clusterconfig.NewConfigFileWatcher(cli.ClusterConfigFile)
	if err != nil {
		log.Fatalf("error loading initial config: %s", err)
	}
	SetConfigWatcher(configWatcher)

	log.Printf("serving hierarchy:\n%s\n", GetClusterConfig().StorageHierarchy.AsciiTree())

	http.HandleFunc("/placement", placementHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/head", headHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/iter_begin", iterBeginHandler)
	http.HandleFunc("/iter_next", iterNextHandler)
	http.HandleFunc("/node_info", nodeInfoHandler)
	http.HandleFunc("/start_scrub", startScrubHandler)
	// cluster internal end points.
	http.HandleFunc("/replicate", replicateHandler)
	http.HandleFunc("/check", checkHandler)

	log.Printf("serving location %v", ThisLocation)
	log.Printf("serving on %s", *listenAddress)

	go ScrubForever()

	err = http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatalf("error serving requests: %s", err)
	}
}
