package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"lukechampine.com/blake3"
)

func mockPutRequest(t *testing.T, query, body string) *http.Request {
	req, err := http.NewRequest("POST", "/put?"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	fw, err := writer.CreateFormFile("data", "data")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fw.Write([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	req.ContentLength = int64(buf.Len())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	bodyF, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Remove(bodyF.Name())
		_ = bodyF.Close()
	})
	_, err = bodyF.Write(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	_, err = bodyF.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	req.Body = bodyF
	return req
}

func mockGetRequest(t *testing.T, query string) *http.Request {
	req, err := http.NewRequest("GET", "/get?"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func mockDeleteRequest(t *testing.T, query string) *http.Request {
	req, err := http.NewRequest("POST", "/delete?"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func mockCheckRequest(t *testing.T, query string) *http.Request {
	req, err := http.NewRequest("GET", "/check?"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestPrimaryPutAndGet(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyForThisLocation(t)

	req := mockGetRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	req = mockPutRequest(t, "key="+k, "hello")
	rr = httptest.NewRecorder()
	putHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 1 {
		t.Fatal("unexpected number of replications")
	}

	req = mockGetRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get request failed: %d", rr.Code)
	}
}

func TestPrimaryDelete(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyForThisLocation(t)

	req := mockCheckRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	checkHandler(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("request failed: %d", rr.Code)
	}

	req = mockDeleteRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	deleteHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 1 {
		t.Fatal("unexpected number of replications")
	}

	req = mockGetRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	req = mockCheckRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	checkHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("request failed: %d", rr.Code)
	}
	responseBody, _ := io.ReadAll(rr.Body)

	meta := ObjMeta{}
	err := json.Unmarshal(responseBody, &meta)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Tombstone {
		t.Fatalf("expected a tombstone, got %v", meta)
	}
}

func TestSecondaryPut(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyForOtherLocation(t)

	req := mockGetRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	objStamp := ObjStamp{
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
	}
	objStampBytes := objStamp.ToBytes()
	objHash := blake3.Sum256(objStampBytes[:])
	obj := [40]byte{}
	copy(obj[0:32], objHash[:])
	copy(obj[32:40], objStampBytes[:])

	req = mockPutRequest(t, "key="+k+"&type=replicate", string(obj[:]))
	rr = httptest.NewRecorder()
	putHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 0 {
		t.Fatal("unexpected number of replications")
	}

	req = mockGetRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get request failed: %d", rr.Code)
	}
}

func TestRejectsCorrupt(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyForThisLocation(t)

	rr := httptest.NewRecorder()
	req := mockPutRequest(t, "key="+k+"&type=replicate", "replicate data with no header xxxxxxxxxxx")
	putHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
}
