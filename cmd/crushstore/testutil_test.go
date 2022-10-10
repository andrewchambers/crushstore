package main

import (
	"errors"
	"fmt"
	mathrand "math/rand"
	"os"
	"testing"

	"github.com/andrewchambers/crushstore/clusterconfig"
	"github.com/andrewchambers/crushstore/crush"
)

var TestConfig = `
cluster-secret: password
storage-schema: host
placement-rules:
    - select host 2
storage-nodes:
    - 100 healthy http://127.0.0.1:5000
    - 100 healthy http://127.0.0.1:5001
    - 100 healthy http://127.0.0.1:5002
`

func PrepareForTest(t *testing.T) {
	ThisLocation = crush.Location{"http://127.0.0.1:5000"}
	err := OpenDataDir(ThisLocation, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	TheNetwork = &MockNetwork{
		ReplicateFunc: func(server string, k string, f *os.File) error { return nil },
		CheckFunc: func(server string, k string) (ObjMeta, bool, error) {
			return ObjMeta{}, false, errors.New("not configured")
		},
	}
	cfg, err := clusterconfig.ParseClusterConfig([]byte(TestConfig))
	if err != nil {
		t.Fatal(err)
	}
	SetClusterConfig(cfg)
}

func RandomKeyForThisLocation(t *testing.T) string {
	cfg := GetClusterConfig()
	for {
		k := fmt.Sprintf("key%d", mathrand.Int())
		loc, err := cfg.Crush(k)
		if err != nil {
			t.Fatal(err)
		}
		if loc[0].Equals(ThisLocation) {
			return k
		}
	}
}

func RandomKeyForOtherLocation(t *testing.T) string {
	cfg := GetClusterConfig()
	for {
		k := fmt.Sprintf("key%d", mathrand.Int())
		loc, err := cfg.Crush(k)
		if err != nil {
			t.Fatal(err)
		}
		if !loc[0].Equals(ThisLocation) {
			return k
		}
	}
}
