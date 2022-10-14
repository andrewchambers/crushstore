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
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbnjay/memory"
	"golang.org/x/sys/unix"
	"lukechampine.com/blake3"
)

func internalError(w http.ResponseWriter, format string, a ...interface{}) {
	log.Printf(format, a...)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("internal server error"))
}

func AuthorizedRequest(req *http.Request) bool {
	cfg := GetClusterConfig()
	authHeader := req.Header.Get("Authorization")
	// Not much point in checking this.
	// if !strings.HasPrefix(authHeader, "Bearer ") {
	// 	return false
	// }
	return strings.HasSuffix(authHeader, cfg.ClusterSecret)
}

func placementHandler(w http.ResponseWriter, req *http.Request) {
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
	locs, err := GetClusterConfig().Crush(k)
	if err != nil {
		internalError(w, "error placing %q: %s", k, err)
		return
	}
	buf, err := json.Marshal(locs)
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
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
		B3sum              B3sum
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

type objectForm struct {
	Key           string
	ObjectPath    string
	ObjectDir     string
	TmpObjectFile *os.File
	QueryFields   url.Values
	Fields        map[string]string
}

func (form *objectForm) FormValue(s string) string {
	v, ok := form.Fields[s]
	if ok {
		return v
	}
	return form.QueryFields.Get(s)
}

func (form *objectForm) Cleanup() {
	if form.TmpObjectFile != nil {
		err := os.Remove(form.TmpObjectFile.Name())
		if err != nil {
			log.Printf("error cleaning up %q: %s", form.TmpObjectFile.Name(), err)
		}
		_ = form.TmpObjectFile.Close()
	}
}

// Our own version because the stdlib doesn't let us read directly into the object directory without a copy.
func readObjectForm(req *http.Request) (*objectForm, bool, error) {

	query := req.URL.Query()
	parsed := &objectForm{
		QueryFields: query,
		Fields:      make(map[string]string),
	}

	cleanupForm := true
	defer func() {
		if cleanupForm {
			parsed.Cleanup()
		}
	}()

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		return nil, false, nil
	}
	d, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, false, nil
	}
	if d != "multipart/form-data" {
		return nil, false, nil
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, false, nil
	}

	r := multipart.NewReader(req.Body, boundary)
	for {
		p, err := r.NextPart()
		if err != nil {
			if err == io.EOF {
				cleanupForm = false
				return parsed, true, nil
			}
			return nil, false, err
		}
		name := p.FormName()
		fileName := p.FileName()
		if fileName != "" {
			if fileName != "data" || parsed.TmpObjectFile != nil {
				return nil, false, nil
			}

			key := parsed.FormValue("key")
			if key == "" {
				return nil, false, nil
			}

			parsed.Key = key
			parsed.ObjectPath = ObjectPathFromKey(key)
			parsed.ObjectDir = filepath.Dir(parsed.ObjectPath)

			tmpF, err := os.CreateTemp(parsed.ObjectDir, "*$tmp")
			if err != nil {
				return nil, false, err
			}
			parsed.TmpObjectFile = tmpF

			_, err = io.Copy(tmpF, p)
			if err != nil {
				return nil, false, err
			}

			_, err = tmpF.Seek(0, io.SeekStart)
			if err != nil {
				return nil, false, err
			}

		} else {
			valueBytes, err := io.ReadAll(p)
			if err != nil {
				return nil, false, err
			}
			value := string(valueBytes)
			parsed.Fields[name] = value
		}
	}
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	objForm, ok, err := readObjectForm(req)
	if err != nil {
		internalError(w, "io error reading post: %s", err)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "bad upload")
		return
	}
	defer objForm.Cleanup()

	k := objForm.Key
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	nReplicas, _ := strconv.Atoi(objForm.FormValue("replicas"))
	objPath := objForm.ObjectPath
	objDir := objForm.ObjectDir
	tmpF := objForm.TmpObjectFile

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
	size, err := io.Copy(hasher, tmpF)
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
		Replicas: nReplicas,
		DoCheck:  false,
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
	objForm.TmpObjectFile = nil

	err = flushDir(objDir)
	if err != nil {
		internalError(w, "io error flushing: %s", err)
	}

}

func deleteHandler(w http.ResponseWriter, req *http.Request) {
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	objForm, ok, err := readObjectForm(req)
	if err != nil {
		internalError(w, "io error reading post: %s", err)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "bad upload")
		return
	}
	defer objForm.Cleanup()

	k := objForm.Key
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cid := objForm.FormValue("cid")
	fanout := objForm.FormValue("fanout") == "true"
	objPath := objForm.ObjectPath
	objDir := objForm.ObjectDir
	tmpF := objForm.TmpObjectFile

	if GetClusterConfig().ConfigId != cid {
		log.Printf("misdirected replication triggering config check")
		_, err := ReloadClusterConfig()
		if err != nil {
			internalError(w, "error reloading config: %s", err)
			return
		}
		w.WriteHeader(http.StatusMisdirectedRequest)
		return
	}

	headerBytes := [OBJECT_HEADER_SIZE]byte{}
	_, err = io.ReadFull(tmpF, headerBytes[:])
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			w.WriteHeader(http.StatusBadRequest)
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

	hasher := blake3.New(32, nil)
	size, err := io.Copy(hasher, tmpF)
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	if header.Size != uint64(size) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("object header size is %d but read %d bytes", header.Size, size))
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

	if fanout {
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
	objForm.TmpObjectFile = nil

	err = flushDir(objDir)
	if err != nil {
		internalError(w, "io error flushing: %s", err)
	}
}

type fanoutOpts struct {
	DoCheck  bool
	Replicas int
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

		if opts.Replicas > len(locs) {
			return fmt.Errorf("unable to satisfy %d replicas with the current placement", opts.Replicas)
		}
		minReplicas := uint64(len(locs))
		if opts.Replicas > 0 {
			minReplicas = uint64(opts.Replicas)
		}

		for i := uint64(0); i < minReplicas; i++ {
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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

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

func nodeInfoHandler(w http.ResponseWriter, req *http.Request) {
	if !AuthorizedRequest(req) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var stat unix.Statfs_t
	err := unix.Statfs(ObjectDir, &stat)
	if err != nil {
		internalError(w, "error getting disk usage: %s", err)
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	scrubRecord := GetLastScrubRecord()
	totalSpace := stat.Blocks * uint64(stat.Frsize)
	freeSpace := stat.Bavail * uint64(stat.Bsize)
	usedSpace := totalSpace - freeSpace

	freeRAM := memory.FreeMemory()

	buf, err := json.Marshal(&struct {
		LastScrubErrorCount  uint64
		HeapAlloc            uint64
		FreeSpace            uint64
		UsedSpace            uint64
		LastScrubSeconds     time.Duration
		LastFullScrubSeconds time.Duration
		FreeRAM              uint64
	}{
		LastScrubDuration:     scrubRecord.LastScrubDuration,
		LastFullScrubDuration: scrubRecord.LastFullScrubDuration,
		LastScrubErrorCount:   scrubRecord.ErrorCount(),
		HeapAlloc:             m.HeapAlloc,
		FreeSpace:             freeSpace,
		UsedSpace:             usedSpace,
		FreeRAM:               freeRAM,
	})
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}
