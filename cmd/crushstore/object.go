package main

import (
	"encoding/binary"
	"time"
)

const OBJ_HEADER_SIZE = 40

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
