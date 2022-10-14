package main

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"lukechampine.com/blake3"
)

func internalError(w http.ResponseWriter, format string, a ...interface{}) {
	log.Printf(format, a...)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("internal server error"))
}

type objectContentReadSeeker struct {
	f *os.File
}

func (of *objectContentReadSeeker) Read(buf []byte) (int, error) {
	return of.f.Read(buf)
}

func (of *objectContentReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		o, err := of.f.Seek(offset, whence)
		return o - OBJECT_HEADER_SIZE, err
	case io.SeekStart:
		o, err := of.f.Seek(offset+OBJECT_HEADER_SIZE, whence)
		return o - OBJECT_HEADER_SIZE, err
	case io.SeekEnd:
		o, err := of.f.Seek(offset, whence)
		return o - OBJECT_HEADER_SIZE, err
	default:
		panic("bad whence")
	}
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	objPath := ObjectPathFromKey(k)
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			locs, err := GetClusterConfig().Crush(k)
			if err != nil {
				internalError(w, "error placing %q: %s", k, err)
				return
			}
			misdirected := true
			for i := 0; i < len(locs); i++ {
				if locs[i].Equals(ThisLocation) {
					misdirected = false
					break
				}
			}
			if misdirected {
				// For get requests we can just redirect to another server.
				randomLoc := locs[mathrand.Int()%len(locs)]
				randomServer := randomLoc[len(randomLoc)-1]
				redirect := fmt.Sprintf("%s/get?key=%s", randomServer, url.QueryEscape(k))
				http.Redirect(w, req, redirect, http.StatusSeeOther)
				return
			} else {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		internalError(w, "io error opening %q: %s", objPath, err)
		return
	}
	defer f.Close()

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(f, headerBytes[:])
	if err != nil {
		internalError(w, "io error reading %q: %s", objPath, err)
		return
	}
	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		log.Printf("WARNING: corrupt object header at %q", objPath)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if header.Tombstone {
		w.WriteHeader(http.StatusGone)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", header.Size))
	modTime := time.UnixMicro(int64(header.CreatedAtUnixMicro))
	http.ServeContent(w, req, k, modTime, &objectContentReadSeeker{f})
}

func headHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	objPath := ObjectPathFromKey(k)
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			locs, err := GetClusterConfig().Crush(k)
			if err != nil {
				internalError(w, "error placing %q: %s", k, err)
				return
			}
			misdirected := true
			for i := 0; i < len(locs); i++ {
				if locs[i].Equals(ThisLocation) {
					misdirected = false
					break
				}
			}
			if misdirected {
				// For get requests we can just redirect to another server.
				randomLoc := locs[mathrand.Int()%len(locs)]
				randomServer := randomLoc[len(randomLoc)-1]
				redirect := fmt.Sprintf("%s/head?key=%s", randomServer, url.QueryEscape(k))
				http.Redirect(w, req, redirect, http.StatusSeeOther)
				return
			} else {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		internalError(w, "io error opening %q: %s", objPath, err)
		return
	}
	defer f.Close()

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(f, headerBytes[:])
	if err != nil {
		internalError(w, "io error reading %q: %s", objPath, err)
		return
	}
	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		log.Printf("WARNING: corrupt object header at %q", objPath)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if header.Tombstone {
		w.WriteHeader(http.StatusGone)
		return
	}

	buf, err := json.Marshal(&struct {
		CreatedAtUnixMicro uint64
		Size               uint64
		B3sum              [32]byte
	}{
		CreatedAtUnixMicro: header.CreatedAtUnixMicro,
		Size:               header.Size,
		B3sum:              header.B3sum,
	})
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func flushDir(dirPath string) error {
	// XXX possible cache opens?
	d, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer d.Close()
	// XXX possible to batch syncs across goroutines?
	return d.Sync()
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	k := req.FormValue("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	objPath := ObjectPathFromKey(k)
	objDir := filepath.Dir(objPath)

	err := req.ParseMultipartForm(1024 * 1024)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("unable to read upload: %s", err))
		return
	}

	dataFile, _, err := req.FormFile("data")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing data field"))
		return
	}
	defer dataFile.Close()

	tmpF, err := os.CreateTemp(objDir, "obj*$tmp")
	if err != nil {
		internalError(w, "io error creating temporary file: %s", err)
		return
	}

	removeTmp := true
	defer func() {
		if removeTmp {
			err := os.Remove(tmpF.Name())
			if err != nil {
				log.Printf("io error removing %q: %s", tmpF.Name(), err)
			}
		}
	}()

	header := ObjHeader{}
	header.Tombstone = false
	header.CreatedAtUnixMicro = uint64(time.Now().UnixMicro())
	nullHdr := [OBJECT_HEADER_SIZE]byte{}
	_, err = tmpF.Write(nullHdr[:])
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	hasher := blake3.New(32, nil)
	size, err := io.Copy(io.MultiWriter(tmpF, hasher), dataFile)
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}
	header.Size = uint64(size)
	copy(header.B3sum[:], hasher.Sum(nil))
	headerBytes := header.ToBytes()
	_, err = tmpF.WriteAt(headerBytes[:], 0)
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	// Do the fanout on the temporary object so the scrubber doesn't interfere.
	err = fanoutObject(k, header, tmpF.Name(), fanoutOpts{
		DoCheck: false,
	})
	if err != nil {
		internalError(w, "unable to fanout %q: %s", k, err)
		return
	}

	err = tmpF.Sync()
	if err != nil {
		internalError(w, "io error syncing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Close()
	if err != nil {
		internalError(w, "io error closing %q: %s", tmpF.Name(), err)
		return
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		internalError(w, "io error renaming %q to %q: %s", tmpF.Name(), objPath, err)
	}
	removeTmp = false

	err = flushDir(objDir)
	if err != nil {
		internalError(w, "io error flushing: %s", err)
	}

}

func deleteHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	k := req.FormValue("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	objPath := ObjectPathFromKey(k)
	objDir := filepath.Dir(objPath)

	tmpF, err := os.CreateTemp(objDir, "obj*$tmp")
	if err != nil {
		internalError(w, "io error creating temporary file: %s", err)
		return
	}
	defer tmpF.Close()
	removeTmp := true
	defer func() {
		if removeTmp {
			err := os.Remove(tmpF.Name())
			if err != nil {
				log.Printf("io removing %q: %s", tmpF.Name(), err)
			}
		}
	}()

	objHeader := ObjHeader{
		Tombstone:          true,
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
		Size:               0,
		B3sum:              blake3.Sum256([]byte{}),
	}
	objHeaderBytes := objHeader.ToBytes()
	_, err = tmpF.Write(objHeaderBytes[:])
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	// Do the fanout on the temporary object so the scrubber doesn't interfere.
	err = fanoutObject(k, objHeader, tmpF.Name(), fanoutOpts{
		DoCheck: false,
	})
	if err != nil {
		internalError(w, "unable to fanout %q: %s", k, err)
		return
	}

	err = tmpF.Sync()
	if err != nil {
		internalError(w, "io error syncing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Close()
	if err != nil {
		internalError(w, "io error closing %q: %s", tmpF.Name(), err)
		return
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		internalError(w, "io error renaming %q to %q: %s", tmpF.Name(), objPath, err)
	}
	removeTmp = false

	err = flushDir(objDir)
	if err != nil {
		internalError(w, "io error flushing: %s", err)
	}

}

func checkHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if GetClusterConfig().ConfigId != q.Get("cid") {
		log.Printf("misdirected check triggering config check")
		_, err := ReloadClusterConfig()
		if err != nil {
			internalError(w, "error reloading config: %s", err)
			return
		}
		w.WriteHeader(http.StatusMisdirectedRequest)
		return
	}

	objPath := ObjectPathFromKey(k)
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		internalError(w, "io error opening %q: %s", objPath, err)
		return
	}
	defer f.Close()
	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(f, headerBytes[:])
	if err != nil {
		internalError(w, "io error reading %q: %s", objPath, err)
		return
	}
	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		log.Printf("WARNING: corrupt object header at %q", objPath)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	buf, err := json.Marshal(&header)
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func replicateHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	k := req.FormValue("key")
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if GetClusterConfig().ConfigId != req.FormValue("cid") {
		log.Printf("misdirected replication triggering config check")
		_, err := ReloadClusterConfig()
		if err != nil {
			internalError(w, "error reloading config: %s", err)
			return
		}
		w.WriteHeader(http.StatusMisdirectedRequest)
		return
	}

	objPath := ObjectPathFromKey(k)
	objDir := filepath.Dir(objPath)

	err := req.ParseMultipartForm(1024 * 1024)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("unable to read upload: %s", err))
		return
	}

	dataFile, _, err := req.FormFile("data")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing data field"))
		return
	}
	defer dataFile.Close()

	tmpF, err := os.CreateTemp(objDir, "obj*$tmp")
	if err != nil {
		internalError(w, "io error creating temporary file: %s", err)
		return
	}

	removeTmp := true
	defer func() {
		if removeTmp {
			err := os.Remove(tmpF.Name())
			if err != nil {
				log.Printf("io error removing %q", tmpF.Name())
			}
		}
	}()

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(dataFile, headerBytes[:])
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			w.WriteHeader(400)
			w.Write([]byte("unexpected EOF"))
			return
		} else {
			internalError(w, "unable to read put object: %s", err)
			return
		}
	}

	header, ok := ObjHeaderFromBytes(headerBytes[:])
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "bad header checksum")
		return
	}

	_, err = tmpF.Write(headerBytes[:])
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	hasher := blake3.New(32, nil)
	size, err := io.Copy(io.MultiWriter(tmpF, hasher), dataFile)
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	if header.Size != uint64(size) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "header size mismatch")
		return
	}

	actualB3sum := [32]byte{}
	copy(actualB3sum[:], hasher.Sum(nil))
	if header.B3sum != actualB3sum {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "sent hash did not equal computed hash")
		return
	}

	existingF, err := os.Open(objPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			internalError(w, "io error opening %q: %s", objPath, err)
			return
		}
	}

	if existingF != nil {
		defer existingF.Close()

		existingHeaderBytes := [OBJECT_HEADER_SIZE]byte{}
		_, err := existingF.ReadAt(existingHeaderBytes[:], 0)
		if err != nil {
			internalError(w, "io error reading %q: %s", objPath, err)
			return
		}

		existingHeader, ok := ObjHeaderFromBytes(existingHeaderBytes[:])
		if !ok {
			internalError(w, "io error reading header of %q", objPath)
			return
		}

		if existingHeader.After(&header) {
			// We got the object, but the existing one is newer.
			w.WriteHeader(http.StatusOK)
			return
		}

		// Fallthrough.
	}

	if req.FormValue("fanout") == "true" {
		// Do the fanout on the temporary object so the scrubber doesn't interfere.
		err = fanoutObject(k, header, tmpF.Name(),
			fanoutOpts{
				DoCheck: true,
			})
		if err != nil {
			internalError(w, "unable to fanout %q: %s", k, err)
			return
		}
	}

	err = tmpF.Sync()
	if err != nil {
		internalError(w, "io error syncing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Close()
	if err != nil {
		internalError(w, "io error closing %q: %s", tmpF.Name(), err)
		return
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		internalError(w, "io error renaming %q to %q: %s", tmpF.Name(), objPath, err)
	}
	removeTmp = false

	err = flushDir(objDir)
	if err != nil {
		internalError(w, "io error flushing: %s", err)
	}
}

type fanoutOpts struct {
	DoCheck bool
}

func fanoutObject(k string, header ObjHeader, objPath string, opts fanoutOpts) error {

	wg := &sync.WaitGroup{}
	successfulReplications := uint64(0)
	misdirectedErrors := uint64(0)

	for {
		successfulReplications = 0
		misdirectedErrors = 0

		clusterConfig := GetClusterConfig()
		locs, err := clusterConfig.Crush(k)
		if err != nil {
			return fmt.Errorf("error placing %q: %s", k, err)
		}

		for i := 0; i < len(locs); i++ {
			loc := locs[i]

			if ThisLocation.Equals(loc) {
				atomic.AddUint64(&successfulReplications, 1)
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				server := loc[len(loc)-1]

				if opts.DoCheck {
					existingHeader, ok, err := CheckObj(clusterConfig, server, k)
					if err != nil {
						log.Printf("error checking %q on %s: %s", k, server, err)
						if err == ErrMisdirectedRequest {
							atomic.AddUint64(&misdirectedErrors, 1)
						}
						return
					}
					if ok && !header.After(&existingHeader) {
						atomic.AddUint64(&successfulReplications, 1)
						return
					}
				}

				objF, err := os.Open(objPath)
				if err != nil {
					log.Printf("io error opening %q: %s", objPath, err)
					return
				}
				defer objF.Close()
				log.Printf("replicating %q to %s", k, server)
				err = ReplicateObj(clusterConfig, server, k, objF, ReplicateOpts{})
				if err != nil {
					log.Printf("error replicating %q to %s: %s", k, server, err)
					if err == ErrMisdirectedRequest {
						atomic.AddUint64(&misdirectedErrors, 1)
					}
					return
				}

				atomic.AddUint64(&successfulReplications, 1)
			}()
		}

		wg.Wait()

		if misdirectedErrors != 0 {
			log.Printf("misdirected fanout triggering config check")
			// We tried to replicate to a server that did not want the object,
			// one of the two has an out of date cluster layout.
			_, err = ReloadClusterConfig()
			if err != nil {
				return err
			}
			continue
		}

		minReplicas := uint64(len(locs))
		if successfulReplications < minReplicas {
			return fmt.Errorf("unable to replicate %q to %d servers", k, minReplicas)
		}
		return nil
	}
}

type expiringIter struct {
	lock         sync.Mutex
	it           interface{}
	cleanupTimer *time.Timer
}

const _ITERATOR_EXPIRY = 30 * time.Second

var _activeIterators sync.Map

func iterBeginHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	idBytes := [16]byte{}
	_, err := cryptorand.Read(idBytes[:])
	if err != nil {
		internalError(w, "error creating object id: %s", err)
		return
	}
	id := hex.EncodeToString(idBytes[:])

	var it interface{}

	switch req.FormValue("type") {
	case "", "objects":
		objIt, err := IterateObjects()
		if err != nil {
			internalError(w, "error creating object iterator: %s", err)
			return
		}
		it = objIt
	case "keys":
		keyIt, err := IterateKeys()
		if err != nil {
			internalError(w, "error creating object iterator: %s", err)
			return
		}
		it = keyIt
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("unknown iterator type"))
		return
	}

	cleanup := time.AfterFunc(_ITERATOR_EXPIRY, func() {
		it, ok := _activeIterators.LoadAndDelete(id)
		if ok {
			_ = it.(*expiringIter).it.(io.Closer).Close()
		}
	})

	_activeIterators.Store(id, &expiringIter{
		it:           it,
		cleanupTimer: cleanup,
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "\"%s\"", id)
}

func iterNextHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	itId := req.FormValue("it")
	_it, ok := _activeIterators.Load(itId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("iterator expired or does not exist"))
		return
	}
	it := _it.(*expiringIter)
	it.lock.Lock()
	defer it.lock.Unlock()
	it.cleanupTimer.Reset(_ITERATOR_EXPIRY)

	const BATCH_SIZE = 8192
	var nItems int
	var items interface{}

	switch it := it.it.(type) {
	case *ObjectIter:
		_items := make([]ObjectIterEntry, 0, BATCH_SIZE)
		for i := 0; i < BATCH_SIZE; i++ {
			ent, ok, err := it.Next()
			if err != nil {
				internalError(w, "error during iteration: %s", err)
				return
			}
			if !ok {
				break
			}
			_items = append(_items, ent)
		}
		nItems = len(_items)
		items = _items
	case *KeyIter:
		_items := make([]string, 0, BATCH_SIZE)
		for i := 0; i < BATCH_SIZE; i++ {
			key, ok, err := it.Next()
			if err != nil {
				internalError(w, "error during iteration: %s", err)
				return
			}
			if !ok {
				break
			}
			_items = append(_items, key)
		}
		nItems = len(_items)
		items = _items
	default:
		panic(it)
	}

	if nItems == 0 {
		_activeIterators.Delete(itId)
		it.cleanupTimer.Stop()
		_ = it.it.(io.Closer).Close()
	}

	buf, err := json.Marshal(items)
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}
