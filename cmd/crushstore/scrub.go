package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

var (
	_scrubTrigger chan struct{} = make(chan struct{})

	_lastScrubbedBytes               uint64
	_lastScrubbedObjects             uint64
	_lastScrubCorruptionErrorCount   uint64
	_lastScrubOtherErrorCount        uint64
	_lastScrubReplicationErrorCount  uint64
	_totalScrubbedBytes              uint64
	_totalScrubbedObjects            uint64
	_totalScrubCorruptionErrorCount  uint64
	_totalScrubOtherErrorCount       uint64
	_totalScrubReplicationErrorCount uint64
	_scrubInProgress                 uint64
	_scrubsCompleted                 uint64
)

const (
	SCRUB_EOTHER = iota
	SCRUB_EREPL
	SCRUB_ECORRUPT
)

func logScrubError(class int, format string, a ...interface{}) {
	switch class {
	case SCRUB_EREPL:
		atomic.AddUint64(&_totalScrubReplicationErrorCount, 1)
	case SCRUB_ECORRUPT:
		atomic.AddUint64(&_totalScrubCorruptionErrorCount, 1)
	default:
		atomic.AddUint64(&_totalScrubOtherErrorCount, 1)
	}
	log.Printf(format, a...)
}

func ScrubObject(objPath string, opts ScrubOpts) {
	log.Printf("scrubbing object at %q", objPath)

	k, err := url.QueryUnescape(filepath.Base(objPath))
	if err != nil {
		log.Printf("scrubber removing %q, not a valid object", objPath)
		err = os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
		}
		return
	}

	locs, err := GetClusterConfig().Crush(k)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to place %q: %s", objPath, err)
		return
	}
	primaryLoc := locs[0]

	objF, err := os.Open(objPath)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to open %q: %s", objPath, err)
		return
	}
	defer objF.Close()

	stat, err := objF.Stat()
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to stat %q: %s", objPath, err)
	}
	if err == nil {
		atomic.AddUint64(&_totalScrubbedBytes, uint64(stat.Size()))
	}

	stampBytes := [8]byte{}
	_, err = objF.ReadAt(stampBytes[:], 32)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to read %q: %s", objPath, err)
	}

	stamp := ObjStampFromBytes(stampBytes[:])

	if opts.Full {
		expectedHash := [32]byte{}
		actualHash := [32]byte{}
		_, err := io.ReadFull(objF, expectedHash[:])
		if err != nil && !errors.Is(err, io.EOF) {
			logScrubError(SCRUB_EOTHER, "io error scrubbing %q: %s", objPath, err)
			return
		}
		hasher := blake3.New(32, nil)
		_, err = io.Copy(hasher, objF)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "io error scrubbing %q: %s", objPath, err)
			return
		}
		copy(actualHash[:], hasher.Sum(nil))
		if expectedHash != actualHash {
			log.Printf("scrub detected corrupt file at %q, removing it", objPath)
			err = os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
			}
			return
		}

		_, err = objF.Seek(0, io.SeekStart)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "io error seeking %q", objPath)
			return
		}

		// We only trust a tombstone after it has been fully scrubbed.
		if stamp.IsExpired(time.Now(), TOMBSTONE_EXPIRY) {
			log.Printf("scrubber removing %q, it has expired", objPath)
			err := os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_EOTHER, "unable to remove %q: %s", objPath, err)
			}
			return
		}

	}

	if ThisLocation.Equals(primaryLoc) {
		for i := 1; i < len(locs); i++ {
			server := locs[i][len(locs[i])-1]
			meta, ok, err := CheckObj(server, k)
			if err != nil {
				logScrubError(SCRUB_EREPL, "scrubber check failed: %s", err)
				continue
			}
			if ok {
				if stamp.Tombstone && !meta.Tombstone {
					// Both have the data, but they disagree about the deletion state.
					log.Printf("scrubber replicating tombstone of %q to %s", k, server)
					err := ReplicateObj(server, k, objF)
					if err != nil {
						logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
					}
				}
			} else {
				log.Printf("scrubber replicating %q to %s", k, server)
				err := ReplicateObj(server, k, objF)
				if err != nil {
					logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				}
			}
		}
	} else {
		primaryServer := primaryLoc[len(primaryLoc)-1]
		meta, ok, err := CheckObj(primaryServer, k)
		if err != nil {
			logScrubError(SCRUB_EREPL, "scrubber was unable to verify primary placement of %q: %s", k, err)
			return
		}
		if !ok || (stamp.Tombstone && !meta.Tombstone) {
			if !ok {
				log.Printf("restoring %q to primary server %s", k, primaryServer)
			} else {
				log.Printf("scrubber replicating tombstone of %q to %s", k, primaryServer)
			}
			err := ReplicateObj(primaryServer, k, objF)
			if err != nil {
				logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				return
			}
		}
		keepObject := false
		for i := 0; i < len(locs); i++ {
			keepObject = keepObject || ThisLocation.Equals(locs[i])
		}
		if !keepObject {
			log.Printf("scrubber removing %q, it has been moved", k)
			err = os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_EOTHER, "unable to remove %q: %s", objPath, err)
			}
		}
	}
}

type ScrubOpts struct {
	Full bool
}

func Scrub(opts ScrubOpts) {
	log.Printf("scrub started, full=%v", opts.Full)
	atomic.StoreUint64(&_scrubInProgress, 1)

	startTotalScrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects)
	startTotalScrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes)
	startTotalReplicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount)
	startTotalCorruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount)
	startTotalOtherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount)

	defer func() {
		scrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects) - startTotalScrubbedObjects
		scrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes) - startTotalScrubbedBytes
		replicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount) - startTotalReplicationErrorCount
		corruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount) - startTotalCorruptionErrorCount
		otherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount) - startTotalOtherErrorCount
		errorCount := replicationErrorCount + corruptionErrorCount + otherErrorCount
		log.Printf("scrubbed %d object(s), %d byte(s) with %d error(s)", scrubbedObjects, scrubbedBytes, errorCount)
		atomic.StoreUint64(&_lastScrubReplicationErrorCount, replicationErrorCount)
		atomic.StoreUint64(&_lastScrubCorruptionErrorCount, corruptionErrorCount)
		atomic.StoreUint64(&_lastScrubOtherErrorCount, otherErrorCount)
		atomic.StoreUint64(&_lastScrubbedBytes, scrubbedBytes)
		atomic.StoreUint64(&_scrubInProgress, 0)
	}()

	dispatch := make(chan string)

	errg, _ := errgroup.WithContext(context.Background())
	const N_SCRUB_WORKERS = 4
	for i := 0; i < N_SCRUB_WORKERS; i++ {
		errg.Go(func() error {
			for {
				path, ok := <-dispatch
				if !ok {
					return nil
				}
				ScrubObject(path, opts)
				atomic.AddUint64(&_totalScrubbedObjects, 1)
			}
		})
	}

	objectCount := uint64(0)
	err := filepath.WalkDir(filepath.Join(DataDir, "obj"), func(path string, e fs.DirEntry, err error) error {
		if e.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, "$tmp") {
			stat, err := os.Stat(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				logScrubError(SCRUB_EOTHER, "error stating temporary file: %s", err)
				return nil
			}
			// Cleanup interrupted puts after a long delay.
			if stat.ModTime().Add(24 * 90 * time.Hour).Before(time.Now()) {
				log.Printf("scrubber removing expired temporary file %q", path)
				err := os.Remove(path)
				if err != nil {
					logScrubError(SCRUB_EOTHER, "error removing %q: %s", path, err)
				}
			}
			return nil
		}
		dispatch <- path
		objectCount += 1
		return nil
	})
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrub walk had an error: %s", err)
	}
	atomic.StoreUint64(&_lastScrubbedObjects, objectCount)

	close(dispatch)
	err = errg.Wait()
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrub worker had an error: %s", err)
	}

	atomic.AddUint64(&_scrubsCompleted, 1)
}

func ScrubForever() {
	full := true // XXX store the state somewhere?
	// XXX config/better/intervals.
	fullScrubTicker := time.NewTicker(5 * time.Minute)
	fastScrubTicker := time.NewTicker(30 * time.Second)
	for {
		startCfg := GetClusterConfig()
		Scrub(ScrubOpts{Full: full}) // XXX config full and not.
		endCfg := GetClusterConfig()
		if startCfg != endCfg {
			// The config changed while scrubbing, we must scrub again with
			// the new config to handle any placement changes.
			full = false
			continue
		}
		select {
		case <-fullScrubTicker.C:
			full = true
		case <-fastScrubTicker.C:
			full = false
		case <-_scrubTrigger:
			full = false
		}
	}
}

func TriggerScrub() bool {
	select {
	case _scrubTrigger <- struct{}{}:
		return true
	default:
		return false
	}
}
