package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
	"github.com/dustin/go-humanize"
)

var (
	ScrubParallelism    = 4
	ScrubTempFileExpiry = 7 * 24 * time.Hour
	ScrubInterval       = 24 * time.Hour
	FullScrubInterval   = 7 * 24 * time.Hour

	_scrubTrigger                    chan struct{} = make(chan struct{}, 1)
	_scrubTriggerForceFull           uint32
	_totalScrubbedBytes              uint64
	_totalScrubbedObjects            uint64
	_totalScrubCorruptionErrorCount  uint64
	_totalScrubOtherErrorCount       uint64
	_totalScrubReplicationErrorCount uint64
	_scrubInProgress                 uint64

	_lastScrubLock sync.Mutex
	_lastScrub     ScrubRecord
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
	k, err := url.QueryUnescape(filepath.Base(objPath))
	if err != nil {
		log.Printf("scrubber removing %q, not a valid object", objPath)
		err = os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
		}
		return
	}

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

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(objF, headerBytes[:])
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to read %q: %s", objPath, err)
	}

	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		log.Printf("scrub detected corrupt file header at %q, removing it", objPath)
		err := os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
		}
		return
	}

	if header.IsExpired(time.Now()) {
		log.Printf("scrubber removing %q, it has expired", objPath)
		err := os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "unable to remove %q: %s", objPath, err)
		}
		return
	}

	if opts.Full {
		actualB3sum := [32]byte{}
		hasher := blake3.New(32, nil)
		_, err = io.Copy(hasher, objF)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "io error scrubbing %q: %s", objPath, err)
			return
		}
		copy(actualB3sum[:], hasher.Sum(nil))
		if actualB3sum != header.B3sum {
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
	}

	var clusterConfig *clusterconfig.ClusterConfig

rebalanceAgain:

	if clusterConfig == nil {
		clusterConfig = GetClusterConfig()
	} else {
		log.Printf("misdirected scrub replication triggering config check")
		newConfig, err := ReloadClusterConfig()
		if err != nil {
			logScrubError(SCRUB_EOTHER, "scrubber unable to reload config: %s", err)
			return
		}
		clusterConfig = newConfig
	}

	locs, err := clusterConfig.Crush(k)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to place %q: %s", objPath, err)
		return
	}
	primaryLoc := locs[0]
	if ThisLocation.Equals(primaryLoc) {
		for i := 1; i < len(locs); i++ {
			loc := locs[i]
			server := loc[len(loc)-1]
			existingHeader, ok, err := CheckObj(clusterConfig, server, k)
			if err != nil {
				if err == ErrMisdirectedRequest {
					goto rebalanceAgain
				}
				logScrubError(SCRUB_EREPL, "scrubber check failed: %s", err)
				continue
			}
			if !ok || header.After(&existingHeader) {
				log.Printf("scrubber replicating %q to %s", k, server)
				err := ReplicateObj(clusterConfig, server, k, objF, ReplicateOptions{})
				if err != nil {
					if err == ErrMisdirectedRequest {
						goto rebalanceAgain
					}
					logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				}
			}
		}
	} else {
		primaryServer := primaryLoc[len(primaryLoc)-1]
		existingHeader, ok, err := CheckObj(clusterConfig, primaryServer, k)
		if err != nil {
			if err == ErrMisdirectedRequest {
				goto rebalanceAgain
			}
			logScrubError(SCRUB_EREPL, "scrubber was unable to verify primary placement of %q: %s", k, err)
			return
		}
		if !ok || header.After(&existingHeader) {
			log.Printf("restoring %q to primary server %s", k, primaryServer)
			err := ReplicateObj(clusterConfig, primaryServer, k, objF, ReplicateOptions{Fanout: true})
			if err != nil {
				if err == ErrMisdirectedRequest {
					goto rebalanceAgain
				}
				logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				return
			}
		}

		// XXX: race condition.
		// Imagine there are just two servers with two object replicas:
		//
		// 1. Config is updated to single replica.
		// 2. This server updates config and decides it no longer wants the object.
		// 3. This server checks the other server has the object and it does.
		// 4. Config is updated again reversing the placement.
		// 5. The other server gets this config first and decides it no longer wants the object.
		// 6. The other server checks this server and finds the object.
		// 7. Both servers delete the object.
		//
		// This race condition seems like it would be extremely unlikely,
		// but we should verify this or find a suitable fix.

		keepObject := false
		for i := 0; i < len(locs); i++ {
			keepObject = keepObject || ThisLocation.Equals(locs[i])
			if keepObject {
				break
			}
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

	scrubRecord := GetLastScrubRecord()
	scrubStart := time.Now()
	startConfigId := GetClusterConfig().ConfigId
	startTotalScrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects)
	startTotalScrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes)
	startTotalReplicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount)
	startTotalCorruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount)
	startTotalOtherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount)

	defer func() {
		scrubEnd := time.Now()
		scrubDuration := scrubEnd.Sub(scrubStart)
		scrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects) - startTotalScrubbedObjects
		scrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes) - startTotalScrubbedBytes
		replicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount) - startTotalReplicationErrorCount
		corruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount) - startTotalCorruptionErrorCount
		otherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount) - startTotalOtherErrorCount

		scrubRecord = ScrubRecord{
			ScrubsCompleted: scrubRecord.ScrubsCompleted,

			LastScrubStartingConfigId:      startConfigId,
			LastScrubUnixMicro:             uint64(scrubStart.UnixMicro()),
			LastScrubDuration:              scrubDuration,
			LastScrubReplicationErrorCount: replicationErrorCount,
			LastScrubCorruptionErrorCount:  corruptionErrorCount,
			LastScrubOtherErrorCount:       otherErrorCount,
			LastScrubBytes:                 scrubbedBytes,
			LastScrubObjects:               scrubbedObjects,

			LastFullScrubUnixMicro:             scrubRecord.LastFullScrubUnixMicro,
			LastFullScrubCorruptionErrorCount:  scrubRecord.LastFullScrubCorruptionErrorCount,
			LastFullScrubReplicationErrorCount: scrubRecord.LastFullScrubReplicationErrorCount,
			LastFullScrubOtherErrorCount:       scrubRecord.LastFullScrubOtherErrorCount,
			LastFullScrubDuration:              scrubRecord.LastFullScrubDuration,
		}

		if opts.Full {
			scrubRecord.LastFullScrubUnixMicro = scrubRecord.LastScrubUnixMicro
			scrubRecord.LastFullScrubDuration = scrubRecord.LastScrubDuration
			scrubRecord.LastFullScrubCorruptionErrorCount = scrubRecord.LastScrubCorruptionErrorCount
			scrubRecord.LastFullScrubReplicationErrorCount = scrubRecord.LastScrubReplicationErrorCount
			scrubRecord.LastFullScrubOtherErrorCount = scrubRecord.LastScrubOtherErrorCount
		}

		SaveScrubRecord(scrubRecord)

		log.Printf(
			"scrubbed %d object(s), %s with %d corruption errors and %d other error(s) in %s",
			scrubbedObjects,
			humanize.IBytes(scrubbedBytes),
			corruptionErrorCount,
			scrubRecord.ErrorCount()-corruptionErrorCount,
			scrubDuration.Truncate(time.Millisecond),
		)
		atomic.StoreUint64(&_scrubInProgress, 0)
	}()

	dispatch := make(chan string)

	errg, _ := errgroup.WithContext(context.Background())

	for i := 0; i < ScrubParallelism; i++ {
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
	err := filepath.WalkDir(filepath.Join(ObjectDir, "obj"), func(path string, e fs.DirEntry, err error) error {
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
			if stat.ModTime().Add(ScrubTempFileExpiry).Before(time.Now()) {
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

	close(dispatch)
	err = errg.Wait()
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrub worker had an error: %s", err)
	}
}

func ScrubForever() {
	LoadLastScrubRecord()

	doScrub := false
	full := false

	scrubTicker := time.NewTicker(ScrubInterval / 2)
	defer scrubTicker.Stop()

	for {
		record := GetLastScrubRecord()

		now := time.Now()
		lastScrub := time.UnixMicro(int64(record.LastScrubUnixMicro))
		lastFullScrub := time.UnixMicro(int64(record.LastFullScrubUnixMicro))

		// Fix any time jumps.
		if lastScrub.After(now) || lastFullScrub.After(now) {
			lastScrub = now
			lastFullScrub = now
		}

		if lastScrub.Add(ScrubInterval).Before(now) {
			doScrub = true
		}

		if record.LastScrubStartingConfigId != GetClusterConfig().ConfigId {
			doScrub = true
		}

		if lastFullScrub.Add(FullScrubInterval).Before(now) {
			doScrub = true
			full = true
		}

		for doScrub {
			Scrub(ScrubOpts{Full: full})
			full = false
			if LastScrubHadErrors() {
				time.Sleep(1 * time.Second)
			} else {
				doScrub = false
			}
		}

		select {
		case <-scrubTicker.C:
		case <-_scrubTrigger:
			doScrub = true
			if atomic.SwapUint32(&_scrubTriggerForceFull, 0) == 1 {
				full = true
			}
		}
	}
}

type TriggerScrubOptions struct {
	FullScrub bool
}

func TriggerScrub(opts TriggerScrubOptions) bool {
	if opts.FullScrub {
		atomic.StoreUint32(&_scrubTriggerForceFull, 1)
	}
	select {
	case _scrubTrigger <- struct{}{}:
		return true
	default:
		return false
	}
}

type ScrubRecord struct {
	ScrubsCompleted uint64

	LastScrubStartingConfigId          string
	LastFullScrubUnixMicro             uint64
	LastFullScrubDuration              time.Duration
	LastFullScrubReplicationErrorCount uint64
	LastFullScrubCorruptionErrorCount  uint64
	LastFullScrubOtherErrorCount       uint64

	LastScrubUnixMicro             uint64
	LastScrubDuration              time.Duration
	LastScrubReplicationErrorCount uint64
	LastScrubCorruptionErrorCount  uint64
	LastScrubOtherErrorCount       uint64
	LastScrubBytes                 uint64
	LastScrubObjects               uint64
}

func (sr *ScrubRecord) ErrorCount() uint64 {
	return sr.LastScrubReplicationErrorCount + sr.LastScrubCorruptionErrorCount + sr.LastScrubOtherErrorCount
}

func LastScrubHadErrors() bool {
	_lastScrubLock.Lock()
	defer _lastScrubLock.Unlock()
	return _lastScrub.ErrorCount() != 0
}

func GetLastScrubRecord() ScrubRecord {
	_lastScrubLock.Lock()
	defer _lastScrubLock.Unlock()
	return _lastScrub
}

func SaveScrubRecord(record ScrubRecord) {
	_lastScrubLock.Lock()
	defer _lastScrubLock.Unlock()
	_lastScrub = record
	recordBytes, err := json.Marshal(&record)
	if err != nil {
		log.Printf("WARNING: unable to marshal scrub record")
		return
	}
	scrubRecordPath := filepath.Join(ObjectDir, "scrub-record")
	tmpScrubRecordPath := scrubRecordPath + "$tmp"
	err = os.WriteFile(tmpScrubRecordPath, recordBytes, 0o644)
	if err != nil {
		log.Printf("WARNING: unable to write new scrub record")
		return
	}
	err = os.Rename(tmpScrubRecordPath, scrubRecordPath)
	if err != nil {
		log.Printf("WARNING: unable to replace scrub record")
	}
}

func LoadLastScrubRecord() {
	record := ScrubRecord{}
	recordJson, err := os.ReadFile(filepath.Join(ObjectDir, "scrub-record"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("WARNING: unable to load last scrub record: %s", err)
		}
		return
	}
	err = json.Unmarshal(recordJson, &record)
	if err != nil {
		log.Printf("WARNING: unable to unmarshal last scrub record: %s", err)
		return
	}
	_lastScrubLock.Lock()
	defer _lastScrubLock.Unlock()
	_lastScrub = record
}

func TotalScrubbedObjects() uint64 {
	return atomic.LoadUint64(&_totalScrubbedObjects)
}

func TotalScrubCorruptionErrorCount() uint64 {
	return atomic.LoadUint64(&_totalScrubCorruptionErrorCount)
}

func TotalScrubReplicationErrorCount() uint64 {
	return atomic.LoadUint64(&_totalScrubReplicationErrorCount)
}

func TotalScrubOtherErrorCount() uint64 {
	return atomic.LoadUint64(&_totalScrubOtherErrorCount)
}
