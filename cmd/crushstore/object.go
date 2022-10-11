package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/andrewchambers/crushstore/crush"
	"github.com/cespare/xxhash/v2"
	"golang.org/x/sys/unix"
)

var (
	ObjectDir      string
	ObjectDirLockF *os.File
)

const (
	TOMBSTONE_EXPIRY   = 120 * time.Second // TODO a real/configurable value.
	OBJECT_DIR_SHARDS  = 4096
	OBJECT_HEADER_SIZE = 40
)

type ObjMeta struct {
	Size               uint64
	Tombstone          bool
	CreatedAtUnixMicro uint64
}

type ObjStamp struct {
	Tombstone          bool
	CreatedAtUnixMicro uint64
}

func ObjStampFromBytes(b []byte) ObjStamp {
	stamp := ObjStamp{}
	stamp.FieldsFromBytes(b[:])
	return stamp
}

func (s *ObjStamp) IsExpired(now time.Time, timeout time.Duration) bool {
	return s.Tombstone && time.UnixMicro(int64(s.CreatedAtUnixMicro)).Add(timeout).Before(now)
}

func (s *ObjStamp) FieldsFromBytes(b []byte) {
	stamp := binary.BigEndian.Uint64(b)
	s.Tombstone = (stamp >> 63) != 0
	s.CreatedAtUnixMicro = (stamp << 1) >> 1
}

func (s *ObjStamp) ToBytes() [8]byte {
	stamp := s.CreatedAtUnixMicro
	if s.Tombstone {
		stamp |= 1 << 63
	}
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], stamp)
	return b
}

type ObjectIterEntry struct {
	Key string
	ObjMeta
}

type ObjectIter struct {
	currentDirIdx uint64
	currentDir    *os.File
	buffer        []ObjectIterEntry
}

func IterateObjects() (*ObjectIter, error) {
	d, err := os.Open(fmt.Sprintf("%s/obj/000", ObjectDir))
	if err != nil {
		return nil, err
	}
	return &ObjectIter{
		currentDirIdx: 0,
		currentDir:    d,
		buffer:        []ObjectIterEntry{},
	}, nil
}

func (it *ObjectIter) Next() (ObjectIterEntry, bool, error) {

	for {
		if it.currentDirIdx == OBJECT_DIR_SHARDS {
			return ObjectIterEntry{}, false, nil
		}

		if len(it.buffer) != 0 {
			item := it.buffer[len(it.buffer)-1]
			it.buffer = it.buffer[:len(it.buffer)-1]
			return item, true, nil
		}
		dirEnts, err := it.currentDir.Readdir(512)
		if len(dirEnts) == 0 {
			if err == nil || errors.Is(err, io.EOF) {
				it.currentDirIdx += 1
				if it.currentDirIdx == OBJECT_DIR_SHARDS {
					continue
				}
				_ = it.currentDir.Close()
				d, err := os.Open(fmt.Sprintf("%s/obj/%03x", ObjectDir, it.currentDirIdx))
				if err != nil {
					return ObjectIterEntry{}, false, err
				}
				it.currentDir = d
				continue
			}
			return ObjectIterEntry{}, false, err
		}
		it.buffer = make([]ObjectIterEntry, 0, len(dirEnts))
		for _, ent := range dirEnts {
			if strings.HasSuffix(ent.Name(), "$tmp") {
				continue
			}
			objf, err := os.Open(fmt.Sprintf("%s/obj/%03x/%s", ObjectDir, it.currentDirIdx, ent.Name()))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return ObjectIterEntry{}, false, err
			}
			stampBytes := [8]byte{}
			_, err = objf.ReadAt(stampBytes[:], 32)
			_ = objf.Close()
			if err != nil {
				return ObjectIterEntry{}, false, err
			}
			stamp := ObjStampFromBytes(stampBytes[:])
			key, err := url.QueryUnescape(ent.Name())
			if err != nil {
				continue
			}
			it.buffer = append(it.buffer, ObjectIterEntry{
				Key: key,
				ObjMeta: ObjMeta{
					Tombstone:          stamp.Tombstone,
					CreatedAtUnixMicro: stamp.CreatedAtUnixMicro,
					Size:               uint64(ent.Size()),
				},
			})
		}
	}
}

func (it *ObjectIter) Close() error {
	return it.currentDir.Close()
}

type KeyIter struct {
	currentDirIdx uint64
	currentDir    *os.File
	buffer        []string
}

func IterateKeys() (*KeyIter, error) {
	d, err := os.Open(fmt.Sprintf("%s/obj/000", ObjectDir))
	if err != nil {
		return nil, err
	}
	return &KeyIter{
		currentDirIdx: 0,
		currentDir:    d,
		buffer:        []string{},
	}, nil
}

func (it *KeyIter) Next() (string, bool, error) {
	for {
		if it.currentDirIdx == OBJECT_DIR_SHARDS {
			return "", false, nil
		}
		if len(it.buffer) != 0 {
			key := it.buffer[len(it.buffer)-1]
			it.buffer = it.buffer[:len(it.buffer)-1]
			return key, true, nil
		}
		dirEnts, err := it.currentDir.ReadDir(512)
		if len(dirEnts) == 0 {
			if err == nil || errors.Is(err, io.EOF) {
				it.currentDirIdx += 1
				if it.currentDirIdx == OBJECT_DIR_SHARDS {
					continue
				}
				_ = it.currentDir.Close()
				d, err := os.Open(fmt.Sprintf("%s/obj/%03x", ObjectDir, it.currentDirIdx))
				if err != nil {
					return "", false, err
				}
				it.currentDir = d
				continue
			}
			return "", false, err
		}
		it.buffer = make([]string, 0, len(dirEnts))
		for _, ent := range dirEnts {
			if strings.HasSuffix(ent.Name(), "$tmp") {
				continue
			}
			key, err := url.QueryUnescape(ent.Name())
			if err != nil {
				continue
			}
			it.buffer = append(it.buffer, key)
		}
	}
}

func (it *KeyIter) Close() error {
	return it.currentDir.Close()
}

func ObjectPathFromKey(k string) string {
	h := xxhash.Sum64String(k) % OBJECT_DIR_SHARDS
	return fmt.Sprintf("%s/obj/%03x/%s", ObjectDir, h, url.QueryEscape(k))
}

func OpenObjectDir(location crush.Location, dir string) error {
	_, err := os.Stat(dir)
	if err != nil {
		return err
	}

	ObjectDirLockF, err = os.Create(filepath.Join(dir, "store.lock"))
	if err != nil {
		return err
	}
	flockT := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	err = unix.FcntlFlock(ObjectDirLockF.Fd(), unix.F_SETLK, &flockT)
	if err != nil {
		return fmt.Errorf("unable to acquire lock: %w", err)
	}

	locationFile := filepath.Join(dir, "location")

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

	for i := 0; i < OBJECT_DIR_SHARDS; i++ {
		p := filepath.Join(dir, fmt.Sprintf("obj/%03x", i))
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
	ObjectDir = dir
	return nil
}
