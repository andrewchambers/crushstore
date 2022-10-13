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

func mockFilePostRequest(t *testing.T, path string, body []byte) *http.Request {
	req, err := http.NewRequest("POST", path, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	fw, err := writer.CreateFormFile("data", "data")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fw.Write(body)
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

func mockReplicateRequest(t *testing.T, query string, body []byte) *http.Request {
	return mockFilePostRequest(t, "/replicate?"+query, body)
}

func mockPutRequest(t *testing.T, query string, body []byte) *http.Request {
	return mockFilePostRequest(t, "/put?"+query, body)
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

func TestPutAndGet(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File, opts ReplicateOpts) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyPrimary(t)

	req := mockGetRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	req = mockPutRequest(t, "key="+k, []byte("hello"))
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

	// Putting the same object twice should replicate again.
	req = mockPutRequest(t, "key="+k, []byte("hello"))
	rr = httptest.NewRecorder()
	putHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 2 {
		t.Fatal("unexpected number of replications")
	}
}

func TestDelete(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File, opts ReplicateOpts) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyPrimary(t)

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
	if rr.Code != http.StatusGone {
		t.Fatalf("get request not as expected: %d", rr.Code)
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

func TestReplicateWithFanout(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	nCheckCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File, opts ReplicateOpts) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}
	TheNetwork.(*MockNetwork).CheckFunc = func(server string, k string) (ObjMeta, bool, error) {
		nCheckCalls += 1
		return ObjMeta{}, false, nil
	}

	k := RandomKeyPrimary(t)

	req := mockGetRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	objHeaderBytes := (&ObjHeader{
		Size:               0,
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
		B3sum:              blake3.Sum256([]byte{}),
	}).ToBytes()

	req = mockReplicateRequest(t, "key="+k+"&fanout=true", objHeaderBytes[:])
	rr = httptest.NewRecorder()
	replicateHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 1 {
		t.Fatal("unexpected number of replications")
	}
	if nCheckCalls != 1 {
		t.Fatal("unexpected number of checks")
	}

	req = mockGetRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	TheNetwork.(*MockNetwork).CheckFunc = func(server string, k string) (ObjMeta, bool, error) {
		nCheckCalls += 1
		return ObjMeta{}, true, nil
	}

	objHeaderBytes = (&ObjHeader{
		Tombstone:          true,
		Size:               0,
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
		B3sum:              blake3.Sum256([]byte{}),
	}).ToBytes()

	req = mockReplicateRequest(t, "key="+k+"&fanout=true", objHeaderBytes[:])
	rr = httptest.NewRecorder()
	replicateHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 2 {
		t.Fatal("unexpected number of replications")
	}
	if nCheckCalls != 2 {
		t.Fatal("unexpected number of checks")
	}

	TheNetwork.(*MockNetwork).CheckFunc = func(server string, k string) (ObjMeta, bool, error) {
		nCheckCalls += 1
		return ObjMeta{Tombstone: true}, true, nil
	}

	req = mockReplicateRequest(t, "key="+k+"&fanout=true", objHeaderBytes[:])
	rr = httptest.NewRecorder()
	replicateHandler(rr, req)
	if rr.Code != http.StatusOK {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
	if nReplicationCalls != 2 {
		t.Fatal("unexpected number of replications")
	}
	if nCheckCalls != 3 {
		t.Fatal("unexpected number of checks")
	}

	req = mockGetRequest(t, "key="+k)
	rr = httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusGone {
		t.Fatalf("get request failed: %d", rr.Code)
	}

}

func TestReplicateNoFanout(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File, opts ReplicateOpts) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeySecondary(t)

	req := mockGetRequest(t, "key="+k)
	rr := httptest.NewRecorder()
	getHandler(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("get request failed: %d", rr.Code)
	}

	objHeader := ObjHeader{
		Size:               0,
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
		B3sum:              blake3.Sum256([]byte{}),
	}
	objHeaderBytes := objHeader.ToBytes()

	req = mockReplicateRequest(t, "key="+k, objHeaderBytes[:])
	rr = httptest.NewRecorder()
	replicateHandler(rr, req)
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

func TestReplicateRejectsCorrupt(t *testing.T) {
	PrepareForTest(t)

	nReplicationCalls := uint64(0)
	TheNetwork.(*MockNetwork).ReplicateFunc = func(server string, k string, f *os.File, opts ReplicateOpts) error {
		atomic.AddUint64(&nReplicationCalls, 1)
		return nil
	}

	k := RandomKeyPrimary(t)

	rr := httptest.NewRecorder()
	req := mockReplicateRequest(t, "key="+k, []byte("replicate data with no header xxxxxxxxxxx"))
	replicateHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
}

func TestReplicateRejectsOther(t *testing.T) {
	PrepareForTest(t)

	k := RandomKeyOther(t)

	rr := httptest.NewRecorder()
	req := mockReplicateRequest(t, "key="+k, []byte{})
	replicateHandler(rr, req)
	if rr.Code != http.StatusMisdirectedRequest {
		body, _ := io.ReadAll(rr.Body)
		t.Logf("body=%s", string(body))
		t.Fatalf("request failed: %d", rr.Code)
	}
}
