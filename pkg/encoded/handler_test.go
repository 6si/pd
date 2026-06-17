// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoded

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	caps := NewFullEncodingCapabilities(1)
	reg.Register(caps)

	got, ok := reg.Get(1)
	re.True(ok)
	re.Equal(uint64(1), got.StoreID)
	re.True(got.HasCapability(CapEncodedStarJoin))

	_, ok = reg.Get(999)
	re.False(ok)
}

func TestRegistry_Unregister(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	reg.Register(NewStoreEncodingCapabilities(1))
	reg.Register(NewStoreEncodingCapabilities(2))
	re.Len(reg.GetAll(), 2)

	reg.Unregister(1)
	re.Len(reg.GetAll(), 1)

	_, ok := reg.Get(1)
	re.False(ok)
}

func TestRegistry_GetClusterStatus(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	reg.Register(NewFullEncodingCapabilities(1))
	reg.Register(NewFullEncodingCapabilities(2))

	status := reg.GetClusterStatus(2)
	re.True(status.AllStoresReady)
	re.Equal(2, status.EncodingEnabledStores)
	re.Equal(uint32(3), status.MinEncodingVersion)
}

func TestRegistry_GetClusterStatusPartial(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	reg.Register(NewStoreEncodingCapabilities(1))
	// Total is 3 but only 1 registered
	status := reg.GetClusterStatus(3)
	re.False(status.AllStoresReady)
	re.Equal(1, status.EncodingEnabledStores)
	re.Equal(3, status.TotalTiFlashStores)
}

func TestHandler_ServeEncodingStatus(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()
	reg.Register(NewFullEncodingCapabilities(1))
	reg.Register(NewFullEncodingCapabilities(2))

	handler := NewHandler(reg, func() int { return 2 })

	req := httptest.NewRequest(http.MethodGet, "/pd/api/v1/encoding/status", nil)
	w := httptest.NewRecorder()
	handler.ServeEncodingStatus(w, req)

	re.Equal(http.StatusOK, w.Code)
	re.Equal("application/json", w.Header().Get("Content-Type"))

	var status ClusterEncodingStatus
	re.NoError(json.Unmarshal(w.Body.Bytes(), &status))
	re.True(status.AllStoresReady)
	re.Equal(2, status.EncodingEnabledStores)
	re.Equal(uint32(3), status.MinEncodingVersion)
	re.Len(status.Stores, 2)
}

func TestHandler_ServeEncodingStatusEmpty(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	handler := NewHandler(reg, func() int { return 0 })

	req := httptest.NewRequest(http.MethodGet, "/pd/api/v1/encoding/status", nil)
	w := httptest.NewRecorder()
	handler.ServeEncodingStatus(w, req)

	re.Equal(http.StatusOK, w.Code)

	var status ClusterEncodingStatus
	re.NoError(json.Unmarshal(w.Body.Bytes(), &status))
	re.False(status.AllStoresReady)
	re.Equal(0, status.EncodingEnabledStores)
}

func TestHandler_ServeRegisterCapabilities(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	handler := NewHandler(reg, func() int { return 3 })

	caps := StoreEncodingCapabilities{
		StoreID:            5,
		Capabilities:       []Capability{CapDictionaryEncoding, CapEncodedFilter},
		MaxDictCardinality: 2048,
		EncodingVersion:    2,
	}
	body, _ := json.Marshal(caps)

	req := httptest.NewRequest(http.MethodPost, "/pd/api/v1/encoding/capabilities", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeRegisterCapabilities(w, req)

	re.Equal(http.StatusOK, w.Code)

	// Verify it was registered
	got, ok := reg.Get(5)
	re.True(ok)
	re.Equal(uint64(5), got.StoreID)
	re.True(got.HasCapability(CapEncodedFilter))
	re.Equal(uint32(2048), got.MaxDictCardinality)
}

func TestHandler_ServeRegisterCapabilitiesBadRequest(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	handler := NewHandler(reg, func() int { return 1 })

	// Invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/pd/api/v1/encoding/capabilities", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	handler.ServeRegisterCapabilities(w, req)
	re.Equal(http.StatusBadRequest, w.Code)
}

func TestHandler_ServeRegisterCapabilitiesZeroStoreID(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	handler := NewHandler(reg, func() int { return 1 })

	caps := StoreEncodingCapabilities{
		StoreID:         0,
		Capabilities:    []Capability{CapDictionaryEncoding},
		EncodingVersion: 1,
	}
	body, _ := json.Marshal(caps)

	req := httptest.NewRequest(http.MethodPost, "/pd/api/v1/encoding/capabilities", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeRegisterCapabilities(w, req)
	re.Equal(http.StatusBadRequest, w.Code)
}

func TestHandler_RoundTrip_RegisterThenStatus(t *testing.T) {
	re := require.New(t)
	reg := NewRegistry()

	handler := NewHandler(reg, func() int { return 2 })

	// Register store 1 (Phase 1)
	caps1 := StoreEncodingCapabilities{
		StoreID:            1,
		Capabilities:       []Capability{CapDictionaryEncoding},
		MaxDictCardinality: 4096,
		EncodingVersion:    1,
	}
	body1, _ := json.Marshal(caps1)
	req1 := httptest.NewRequest(http.MethodPost, "/pd/api/v1/encoding/capabilities", bytes.NewReader(body1))
	w1 := httptest.NewRecorder()
	handler.ServeRegisterCapabilities(w1, req1)
	re.Equal(http.StatusOK, w1.Code)

	// Register store 2 (Phase 3)
	caps2 := StoreEncodingCapabilities{
		StoreID:            2,
		Capabilities:       []Capability{CapDictionaryEncoding, CapEncodedFilter, CapEncodedGroupBy, CapEncodedBloomFilter, CapEncodedStarJoin},
		MaxDictCardinality: 4096,
		EncodingVersion:    3,
	}
	body2, _ := json.Marshal(caps2)
	req2 := httptest.NewRequest(http.MethodPost, "/pd/api/v1/encoding/capabilities", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	handler.ServeRegisterCapabilities(w2, req2)
	re.Equal(http.StatusOK, w2.Code)

	// Get status: min version should be 1 (store 1 is v1)
	reqStatus := httptest.NewRequest(http.MethodGet, "/pd/api/v1/encoding/status", nil)
	wStatus := httptest.NewRecorder()
	handler.ServeEncodingStatus(wStatus, reqStatus)

	var status ClusterEncodingStatus
	re.NoError(json.Unmarshal(wStatus.Body.Bytes(), &status))
	re.True(status.AllStoresReady)
	re.Equal(2, status.EncodingEnabledStores)
	re.Equal(uint32(1), status.MinEncodingVersion) // Min of v1 and v3
	re.Len(status.Stores, 2)

	// Cluster supports Phase 1 but not Phase 2+
	re.True(status.ClusterSupportsPhase(1))
	re.False(status.ClusterSupportsPhase(2))
}
