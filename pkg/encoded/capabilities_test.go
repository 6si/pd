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

func TestNewFullEncodingCapabilities(t *testing.T) {
	re := require.New(t)

	caps := NewFullEncodingCapabilities(5)
	re.Equal(uint64(5), caps.StoreID)
	re.Equal(uint32(3), caps.EncodingVersion)
	re.True(caps.HasCapability(CapDictionaryEncoding))
	re.True(caps.HasCapability(CapEncodedFilter))
	re.True(caps.HasCapability(CapEncodedGroupBy))
	re.True(caps.HasCapability(CapEncodedBloomFilter))
	re.True(caps.HasCapability(CapEncodedStarJoin))
	re.True(caps.SupportsEncodedOperations())
}

func TestSupportsPhase(t *testing.T) {
	re := require.New(t)

	caps := NewStoreEncodingCapabilities(1) // Version 1
	re.True(caps.SupportsPhase(1))
	re.False(caps.SupportsPhase(2))
	re.False(caps.SupportsPhase(3))

	caps2 := NewFullEncodingCapabilities(2) // Version 3
	re.True(caps2.SupportsPhase(1))
	re.True(caps2.SupportsPhase(2))
	re.True(caps2.SupportsPhase(3))
}

func TestClusterSupportsPhase(t *testing.T) {
	re := require.New(t)

	stores := []*StoreEncodingCapabilities{
		NewFullEncodingCapabilities(1),
		NewFullEncodingCapabilities(2),
	}
	status := ComputeClusterStatus(stores, 2)
	re.True(status.ClusterSupportsPhase(1))
	re.True(status.ClusterSupportsPhase(2))
	re.True(status.ClusterSupportsPhase(3))

	// Mixed: one v1, one v3 → min is 1
	stores2 := []*StoreEncodingCapabilities{
		NewStoreEncodingCapabilities(1),   // v1
		NewFullEncodingCapabilities(2),    // v3
	}
	status2 := ComputeClusterStatus(stores2, 2)
	re.True(status2.ClusterSupportsPhase(1))
	re.False(status2.ClusterSupportsPhase(2))
	re.False(status2.ClusterSupportsPhase(3))
}

func TestCheckOperationFeasibility(t *testing.T) {
	re := require.New(t)

	stores := []*StoreEncodingCapabilities{
		NewFullEncodingCapabilities(1),
		NewFullEncodingCapabilities(2),
		NewFullEncodingCapabilities(3),
	}

	result := CheckOperationFeasibility(stores, 3, CapEncodedStarJoin)
	re.True(result.Feasible)
	re.Equal(3, result.StoresReady)
	re.Equal(3, result.StoresTotal)
}

func TestCheckOperationFeasibilityPartial(t *testing.T) {
	re := require.New(t)

	stores := []*StoreEncodingCapabilities{
		NewFullEncodingCapabilities(1),    // has star join
		NewStoreEncodingCapabilities(2),   // no star join
	}

	result := CheckOperationFeasibility(stores, 2, CapEncodedStarJoin)
	re.False(result.Feasible)
	re.Equal(1, result.StoresReady)
	re.Equal(2, result.StoresTotal)
}

func TestAllOperationsFeasibility(t *testing.T) {
	re := require.New(t)

	stores := []*StoreEncodingCapabilities{
		NewFullEncodingCapabilities(1),
		NewFullEncodingCapabilities(2),
	}

	results := AllOperationsFeasibility(stores, 2)
	re.Len(results, 5) // 5 operations
	for _, r := range results {
		re.True(r.Feasible)
		re.Equal(2, r.StoresReady)
	}
}

func TestPhaseCapabilities(t *testing.T) {
	re := require.New(t)

	re.Len(PhaseCapabilities[1], 1)
	re.Len(PhaseCapabilities[2], 3)
	re.Len(PhaseCapabilities[3], 5)
	re.Contains(PhaseCapabilities[3], CapEncodedStarJoin)
}
