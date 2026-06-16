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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreEncodingCapabilities(t *testing.T) {
	re := require.New(t)

	// Test NewStoreEncodingCapabilities defaults
	caps := NewStoreEncodingCapabilities(1)
	re.Equal(uint64(1), caps.StoreID)
	re.Equal(uint32(4096), caps.MaxDictCardinality)
	re.Equal(uint32(1), caps.EncodingVersion)
	re.True(caps.HasCapability(CapDictionaryEncoding))
	re.False(caps.HasCapability(CapEncodedFilter))
	re.False(caps.HasCapability(CapEncodedGroupBy))
	re.False(caps.SupportsEncodedOperations())
}

func TestStoreEncodingCapabilitiesWithOps(t *testing.T) {
	re := require.New(t)

	caps := &StoreEncodingCapabilities{
		StoreID:            2,
		Capabilities:       []Capability{CapDictionaryEncoding, CapEncodedFilter, CapEncodedGroupBy},
		MaxDictCardinality: 8192,
		EncodingVersion:    2,
	}
	re.True(caps.HasCapability(CapDictionaryEncoding))
	re.True(caps.HasCapability(CapEncodedFilter))
	re.True(caps.HasCapability(CapEncodedGroupBy))
	re.False(caps.HasCapability(CapEncodedStarJoin))
	re.True(caps.SupportsEncodedOperations())
}

func TestClusterEncodingStatus(t *testing.T) {
	re := require.New(t)

	// All stores ready
	stores := []*StoreEncodingCapabilities{
		NewStoreEncodingCapabilities(1),
		NewStoreEncodingCapabilities(2),
		NewStoreEncodingCapabilities(3),
	}
	status := ComputeClusterStatus(stores, 3)
	re.True(status.AllStoresReady)
	re.Equal(3, status.EncodingEnabledStores)
	re.Equal(3, status.TotalTiFlashStores)
	re.Equal(uint32(1), status.MinEncodingVersion)
}

func TestClusterEncodingStatusPartial(t *testing.T) {
	re := require.New(t)

	// Only 2 of 3 stores report encoding capabilities
	stores := []*StoreEncodingCapabilities{
		NewStoreEncodingCapabilities(1),
		NewStoreEncodingCapabilities(2),
	}
	status := ComputeClusterStatus(stores, 3)
	re.False(status.AllStoresReady)
	re.Equal(2, status.EncodingEnabledStores)
	re.Equal(3, status.TotalTiFlashStores)
}

func TestClusterEncodingStatusMixedVersions(t *testing.T) {
	re := require.New(t)

	stores := []*StoreEncodingCapabilities{
		{StoreID: 1, Capabilities: []Capability{CapDictionaryEncoding}, EncodingVersion: 2},
		{StoreID: 2, Capabilities: []Capability{CapDictionaryEncoding}, EncodingVersion: 1},
		{StoreID: 3, Capabilities: []Capability{CapDictionaryEncoding}, EncodingVersion: 3},
	}
	status := ComputeClusterStatus(stores, 3)
	re.True(status.AllStoresReady)
	// MinEncodingVersion should be the lowest
	re.Equal(uint32(1), status.MinEncodingVersion)
}

func TestClusterEncodingStatusEmpty(t *testing.T) {
	re := require.New(t)

	status := ComputeClusterStatus(nil, 0)
	re.False(status.AllStoresReady)
	re.Equal(0, status.EncodingEnabledStores)
	re.Equal(uint32(0), status.MinEncodingVersion)
}
