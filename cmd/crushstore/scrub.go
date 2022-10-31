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
	"github.com/dustin/go-humanize"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

type ScrubErrorTotals struct {
	Corruption  uint64
	Replication uint64
	IO          uint64
	Other       uint64
}

type ScrubRecord struct {
	ScrubsCompleted uint64

	LastFullScrubUnixMicro             uint64
	LastFullScrubDuration              time.Duration
	LastFullScrubReplicationErrorCount uint64
	LastFullScrubCorruptionErrorCount  uint64
	LastFullScrubIOErrorCount          uint64
	LastFullScrubOtherErrorCount       uint64

	LastScrubUnixMicro             uint64
	LastScrubDuration              time.Duration
	LastScrubReplicationErrorCount uint64
	LastScrubCorruptionErrorCount  uint64
	LastScrubIOErrorCount          uint64
	LastScrubOtherErrorCount       uint64
	LastScrubBytes                 uint64
	LastScrubObjects               uint64
}

func (sr *ScrubRecord) ErrorCount() uint64 {
	return sr.LastScrubReplicationErrorCount + sr.LastScrubCorruptionErrorCount + sr.LastScrubIOErrorCount + sr.LastScrubOtherErrorCount
}

var (
	ScrubParallelism    = 4
	ScrubTempFileExpiry = 7 * 24 * time.Hour
	ScrubInterval       = 24 * time.Hour
	FullScrubInterval   = 7 * 24 * time.Hour

	_scrubTrigger             chan struct{} = make(chan struct{}, 1)
	_scrubTriggerForceFull    uint32
	_totalScrubbedBytes       uint64
	_totalScrubbedObjects     uint64
	_scrubInProgress          uint64
	_scrubConfigChangeCounter uint64
	_scrubErrorTotals         ScrubErrorTotals

	_lastScrubLock sync.Mutex
	_lastScrub     ScrubRecord
)

func TotalScrubbedObjects() uint64 {
	return atomic.LoadUint64(&_totalScrubbedObjects)
}

func TotalScrubCorruptionErrorCount() uint64 {
	return atomic.LoadUint64(&_scrubErrorTotals.Corruption)
}

func TotalScrubReplicationErrorCount() uint64 {
	return atomic.LoadUint64(&_scrubErrorTotals.Replication)
}

func TotalScrubIOErrorCount() uint64 {
	return atomic.LoadUint64(&_scrubErrorTotals.IO)
}

func TotalScrubOtherErrorCount() uint64 {
	return atomic.LoadUint64(&_scrubErrorTotals.Other)
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
	// XXX CRC
	_lastScrubLock.Lock()
	defer _lastScrubLock.Unlock()
	_lastScrub = record
	recordBytes, err := json.Marshal(&record)
	if err != nil {
		log.Printf("WARNING: unable to marshal scrub record")
		return
	}
	scrubRecordPath := filepath.Join(ObjectDir, "last-scrub.json")
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
	recordJson, err := os.ReadFile(filepath.Join(ObjectDir, "last-scrub.json"))
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

func SaveScrubErrorTotals() {
	// XXX CRC
	bytes, err := json.Marshal(&_scrubErrorTotals)
	if err != nil {
		log.Printf("WARNING: unable to marshal scrub error totals")
		return
	}
	scrubErrorsPath := filepath.Join(ObjectDir, "scrub-error-totals.json")
	tmpScrubRecordPath := scrubErrorsPath + "$tmp"
	err = os.WriteFile(tmpScrubRecordPath, bytes, 0o644)
	if err != nil {
		log.Printf("WARNING: unable to write new scrub error totals")
		return
	}
	err = os.Rename(tmpScrubRecordPath, scrubErrorsPath)
	if err != nil {
		log.Printf("WARNING: unable to replace scrub error totals")
	}
}

func LoadScrubErrorTotals() {
	bytes, err := os.ReadFile(filepath.Join(ObjectDir, "scrub-error-totals.json"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("WARNING: unable to load scrub error totals: %s", err)
		}
		return
	}
	err = json.Unmarshal(bytes, &_scrubErrorTotals)
	if err != nil {
		log.Printf("WARNING: unable to unmarshal scrub error totals: %s", err)
		return
	}
}

const (
	SCRUB_EOTHER = iota
	SCRUB_EIO
	SCRUB_EREPL
	SCRUB_ECORRUPT
)

func logScrubError(class int, format string, a ...interface{}) {
	switch class {
	case SCRUB_EREPL:
		atomic.AddUint64(&_scrubErrorTotals.Replication, 1)
	case SCRUB_ECORRUPT:
		atomic.AddUint64(&_scrubErrorTotals.Corruption, 1)
	case SCRUB_EIO:
		atomic.AddUint64(&_scrubErrorTotals.IO, 1)
	default:
		atomic.AddUint64(&_scrubErrorTotals.Other, 1)
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
		logScrubError(SCRUB_EIO, "scrubber unable to open %q: %s", objPath, err)
		return
	}
	defer objF.Close()

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(objF, headerBytes[:])
	if err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			logScrubError(SCRUB_EIO, "scrubber unable to read %q: %s", objPath, err)
			return
		}
	}

	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		logScrubError(SCRUB_ECORRUPT, "scrub detected corrupt file header at %q, removing it", objPath)
		err := os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_EIO, "io error removing %q: %s", objPath, err)
		}
		return
	}

	atomic.AddUint64(&_totalScrubbedBytes, header.Size)

	if header.IsExpired(time.Now()) {
		log.Printf("scrubber removing %q, it has expired", objPath)
		err := os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_EIO, "unable to remove %q: %s", objPath, err)
		}
		return
	}

	if opts.Full {
		actualB3sum := [32]byte{}
		hasher := blake3.New(32, nil)
		_, err = io.Copy(hasher, objF)
		if err != nil {
			logScrubError(SCRUB_EIO, "io error scrubbing %q: %s", objPath, err)
			return
		}
		copy(actualB3sum[:], hasher.Sum(nil))
		if actualB3sum != header.B3sum {
			logScrubError(SCRUB_ECORRUPT, "scrub detected corrupt file at %q, removing it", objPath)
			err = os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_EIO, "io error removing %q: %s", objPath, err)
			}
			return
		}

		_, err = objF.Seek(0, io.SeekStart)
		if err != nil {
			logScrubError(SCRUB_EIO, "io error seeking %q", objPath)
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
			err := ReplicateObj(clusterConfig, primaryServer, k, objF, ReplicateOptions{
				Fanout: true, // We want the primary server to immediately fan it out.
			})
			if err != nil {
				if err == ErrMisdirectedRequest {
					goto rebalanceAgain
				}
				logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				return
			}
		}

		// N.B.: race condition.
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
		// Because servers check the configs agree in replicate and check messages, this
		// seems extremely unlikely, if not impossible and in the future would love some formal proof
		// this is impossible using something like TLA+.
		//
		// To be extra safe, we do not automated rapid cycling of the config in the auto-admin.
		// It also seems unlikely human administrators would do this themselves - but we should
		// document that rapidly cycling the configuration before rebalancing is complete is a bad idea
		// and in what cases it may be unsafe.

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
				logScrubError(SCRUB_EIO, "unable to remove %q: %s", objPath, err)
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
	startConfigCounter := atomic.LoadUint64(&_configChangeCounter)
	startTotalScrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects)
	startTotalScrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes)
	startTotalReplicationErrorCount := TotalScrubReplicationErrorCount()
	startTotalCorruptionErrorCount := TotalScrubCorruptionErrorCount()
	startTotalIOErrorCount := TotalScrubIOErrorCount()
	startTotalOtherErrorCount := TotalScrubOtherErrorCount()

	defer func() {
		scrubEnd := time.Now()
		scrubDuration := scrubEnd.Sub(scrubStart)
		scrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects) - startTotalScrubbedObjects
		scrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes) - startTotalScrubbedBytes
		replicationErrorCount := TotalScrubReplicationErrorCount() - startTotalReplicationErrorCount
		corruptionErrorCount := TotalScrubCorruptionErrorCount() - startTotalCorruptionErrorCount
		ioErrorCount := TotalScrubIOErrorCount() - startTotalIOErrorCount
		otherErrorCount := TotalScrubOtherErrorCount() - startTotalOtherErrorCount

		scrubRecord = ScrubRecord{
			ScrubsCompleted: scrubRecord.ScrubsCompleted,

			LastScrubUnixMicro:             uint64(scrubStart.UnixMicro()),
			LastScrubDuration:              scrubDuration,
			LastScrubReplicationErrorCount: replicationErrorCount,
			LastScrubCorruptionErrorCount:  corruptionErrorCount,
			LastScrubOtherErrorCount:       otherErrorCount,
			LastScrubIOErrorCount:          ioErrorCount,
			LastScrubBytes:                 scrubbedBytes,
			LastScrubObjects:               scrubbedObjects,

			LastFullScrubUnixMicro:             scrubRecord.LastFullScrubUnixMicro,
			LastFullScrubCorruptionErrorCount:  scrubRecord.LastFullScrubCorruptionErrorCount,
			LastFullScrubReplicationErrorCount: scrubRecord.LastFullScrubReplicationErrorCount,
			LastFullScrubIOErrorCount:          scrubRecord.LastFullScrubIOErrorCount,
			LastFullScrubOtherErrorCount:       scrubRecord.LastFullScrubOtherErrorCount,
			LastFullScrubDuration:              scrubRecord.LastFullScrubDuration,
		}

		if opts.Full {
			scrubRecord.LastFullScrubUnixMicro = scrubRecord.LastScrubUnixMicro
			scrubRecord.LastFullScrubDuration = scrubRecord.LastScrubDuration
			scrubRecord.LastFullScrubCorruptionErrorCount = scrubRecord.LastScrubCorruptionErrorCount
			scrubRecord.LastFullScrubReplicationErrorCount = scrubRecord.LastScrubReplicationErrorCount
			scrubRecord.LastFullScrubIOErrorCount = scrubRecord.LastScrubIOErrorCount
			scrubRecord.LastFullScrubOtherErrorCount = scrubRecord.LastScrubOtherErrorCount
		}

		SaveScrubRecord(scrubRecord)
		SaveScrubErrorTotals()
		log.Printf(
			"scrubbed %d object(s), %s with %d corruption errors, %d io errors and %d other error(s) in %s",
			scrubbedObjects,
			humanize.IBytes(scrubbedBytes),
			corruptionErrorCount,
			ioErrorCount,
			scrubRecord.ErrorCount()-ioErrorCount-corruptionErrorCount,
			scrubDuration.Truncate(time.Millisecond),
		)
		atomic.StoreUint64(&_scrubConfigChangeCounter, startConfigCounter)
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
	LoadScrubErrorTotals()

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

		if atomic.LoadUint64(&_scrubConfigChangeCounter) != atomic.LoadUint64(&_configChangeCounter) {
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
