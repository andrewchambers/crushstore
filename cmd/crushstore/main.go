package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/andrewchambers/crushstore/crush"
	"github.com/google/shlex"
	"golang.org/x/sys/unix"
)

const (
	TOMBSTONE_EXPIRY = 120 * time.Second // TODO a real/configurable value.
	DATA_DIRSHARDS   = 4096
)

var (
	DataDir      string
	DataDirLockF *os.File
	ThisLocation crush.Location
)

func OpenDataDir(location crush.Location, dataDir string) error {
	_, err := os.Stat(dataDir)
	if err != nil {
		return err
	}

	DataDirLockF, err = os.Create(filepath.Join(dataDir, "store.lock"))
	if err != nil {
		return err
	}
	flockT := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	err = unix.FcntlFlock(DataDirLockF.Fd(), unix.F_SETLK, &flockT)
	if err != nil {
		return fmt.Errorf("unable to acquire lock: %w", err)
	}

	locationFile := filepath.Join(dataDir, "location")

	locationBytes, err := json.Marshal(location)
	if err != nil {
		return err
	}

	_, err = os.Stat(locationFile)
	if err == nil {
		expectedLocationBytes, err := os.ReadFile(locationFile)
		if err != nil {
			return err
		}
		if !bytes.Equal(locationBytes, expectedLocationBytes) {
			return fmt.Errorf(
				"store was last served at location %s, but now is at %s (manually remove %q to allow).",
				string(expectedLocationBytes),
				string(locationBytes),
				locationFile,
			)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		os.WriteFile(locationFile, locationBytes, 0o755)
	} else {
		return err
	}

	// TODO load lastscrub.json
	for i := 0; i < DATA_DIRSHARDS; i++ {
		p := filepath.Join(dataDir, fmt.Sprintf("obj/%03x", i))
		_, err := os.Stat(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				err := os.MkdirAll(p, 0o755)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	DataDir = dataDir
	return nil
}

func main() {

	listenAddress := flag.String("listen-address", "", "Address to listen on.")
	location := flag.String("location", "", "Storage location specification, defaults to http://${listen-address}.")
	dataDir := flag.String("data-dir", "", "Directory to store objects under.")
	clusterConfigFile := flag.String("cluster-config", "./crushstore-cluster.conf", "Directory to store objects under.")

	flag.Parse()

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

	if *dataDir == "" {
		log.Fatalf("-data-dir not specified.")
	}
	err = OpenDataDir(ThisLocation, *dataDir)
	if err != nil {
		log.Fatalf("error preparing -data-dir: %s", err)
	}

	err = ReloadClusterConfigFromFile(*clusterConfigFile)
	if err != nil {
		log.Fatalf("error loading initial config: %s", err)
	}
	log.Printf("serving hierarchy:\n%s\n", GetClusterConfig().StorageHierarchy.AsciiTree())

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/check", checkHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/node_info", nodeInfoHandler)

	log.Printf("serving location %v", ThisLocation)
	log.Printf("serving on %s", *listenAddress)

	go WatchClusterConfigForever(*clusterConfigFile)
	go ScrubForever()

	err = http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatalf("error serving requests: %s", err)
	}
}
