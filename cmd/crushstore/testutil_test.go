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

type MockConfigWatcher struct {
	config *clusterconfig.ClusterConfig
}

func (w *MockConfigWatcher) GetCurrentConfig() *clusterconfig.ClusterConfig {
	return w.config
}

func (w *MockConfigWatcher) OnConfigChange(cb func(cfg *clusterconfig.ClusterConfig, err error)) {
}

func (w *MockConfigWatcher) ReloadConfig() (*clusterconfig.ClusterConfig, error) {
	return w.config, nil
}

func (w *MockConfigWatcher) Stop() {
}

func PrepareForTest(t *testing.T) {
	ThisLocation = crush.Location{"http://127.0.0.1:5000"}
	err := OpenObjectDir(ThisLocation, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	TheNetwork = &MockNetwork{
		ReplicateFunc: func(clusterConfig *clusterconfig.ClusterConfig, server string, k string, f *os.File, opts ReplicateOpts) error {
			return nil
		},
		CheckFunc: func(clusterConfig *clusterconfig.ClusterConfig, server string, k string) (ObjHeader, bool, error) {
			return ObjHeader{}, false, errors.New("not configured")
		},
	}
	config, err := clusterconfig.ParseClusterConfig([]byte(TestConfig))
	if err != nil {
		t.Fatal(err)
	}
	SetConfigWatcher(&MockConfigWatcher{
		config: config,
	})
}

func RandomKeyPrimary(t *testing.T) string {
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

func RandomKeySecondary(t *testing.T) string {
	cfg := GetClusterConfig()
	for {
		k := fmt.Sprintf("key%d", mathrand.Int())
		locs, err := cfg.Crush(k)
		if err != nil {
			t.Fatal(err)
		}
		if !locs[0].Equals(ThisLocation) && locs[1].Equals(ThisLocation) {
			return k
		}
	}
}

func RandomKeyOther(t *testing.T) string {
	cfg := GetClusterConfig()
	for {
		k := fmt.Sprintf("key%d", mathrand.Int())
		locs, err := cfg.Crush(k)
		if err != nil {
			t.Fatal(err)
		}
		if !locs[0].Equals(ThisLocation) && !locs[1].Equals(ThisLocation) {
			return k
		}
	}
}
