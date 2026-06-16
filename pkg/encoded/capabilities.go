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

// Package encoded provides encoding capability metadata for TiFlash stores.
// This is part of the dictionary-encoded columnstore operations feature (Phase 1).
//
// When TiFlash nodes support dictionary encoding, they report their capabilities
// via the store heartbeat. PD tracks which stores support encoded operations,
// enabling TiDB's planner to make informed decisions about which TiFlash nodes
// can execute encoded operations.
package encoded

// Capability represents a single encoding capability supported by a store.
type Capability string

const (
	// CapDictionaryEncoding indicates the store supports dictionary encoding
	// for low-cardinality columns in the columnar storage format.
	CapDictionaryEncoding Capability = "dictionary_encoding"

	// CapEncodedFilter indicates the store can perform filter operations
	// directly on dictionary-encoded data without decoding.
	CapEncodedFilter Capability = "encoded_filter"

	// CapEncodedGroupBy indicates the store can perform group-by operations
	// using dictionary IDs as direct array indices.
	CapEncodedGroupBy Capability = "encoded_group_by"

	// CapEncodedBloomFilter indicates the store supports bloom filter pushdown
	// on dictionary-encoded columns.
	CapEncodedBloomFilter Capability = "encoded_bloom_filter"

	// CapEncodedStarJoin indicates the store supports encoded star join
	// operations (hash join on encoded fact table columns).
	CapEncodedStarJoin Capability = "encoded_star_join"
)

// StoreEncodingCapabilities tracks what encoding operations a TiFlash store supports.
type StoreEncodingCapabilities struct {
	// StoreID is the PD store ID for the TiFlash node.
	StoreID uint64 `json:"store_id"`

	// Capabilities lists the encoding operations this store supports.
	Capabilities []Capability `json:"capabilities"`

	// MaxDictCardinality is the maximum dictionary size this store handles.
	MaxDictCardinality uint32 `json:"max_dict_cardinality"`

	// EncodingVersion indicates the protocol version for encoded operations.
	// Version 1: Dictionary encoding (Phase 1)
	// Version 2: Encoded filter + group-by (Phase 2-3)
	// Version 3: Encoded star joins (Phase 4)
	EncodingVersion uint32 `json:"encoding_version"`
}

// HasCapability checks if a store supports a given encoding capability.
func (s *StoreEncodingCapabilities) HasCapability(cap Capability) bool {
	for _, c := range s.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// SupportsEncodedOperations returns true if the store has any encoded operation
// capabilities beyond basic dictionary encoding.
func (s *StoreEncodingCapabilities) SupportsEncodedOperations() bool {
	return s.HasCapability(CapEncodedFilter) ||
		s.HasCapability(CapEncodedGroupBy) ||
		s.HasCapability(CapEncodedBloomFilter) ||
		s.HasCapability(CapEncodedStarJoin)
}

// NewStoreEncodingCapabilities creates capabilities for a store that supports
// Phase 1 dictionary encoding.
func NewStoreEncodingCapabilities(storeID uint64) *StoreEncodingCapabilities {
	return &StoreEncodingCapabilities{
		StoreID:            storeID,
		Capabilities:       []Capability{CapDictionaryEncoding},
		MaxDictCardinality: 4096,
		EncodingVersion:    1,
	}
}

// ClusterEncodingStatus provides a cluster-wide view of encoding capabilities.
type ClusterEncodingStatus struct {
	// TotalTiFlashStores is the number of TiFlash stores in the cluster.
	TotalTiFlashStores int `json:"total_tiflash_stores"`

	// EncodingEnabledStores is the number of TiFlash stores that support
	// dictionary encoding.
	EncodingEnabledStores int `json:"encoding_enabled_stores"`

	// AllStoresReady is true when all TiFlash stores support dictionary encoding.
	// This is the precondition for enabling dictionary encoding writes.
	AllStoresReady bool `json:"all_stores_ready"`

	// MinEncodingVersion is the minimum encoding version across all stores.
	// TiDB should only enable features up to this version.
	MinEncodingVersion uint32 `json:"min_encoding_version"`

	// Stores contains per-store capability details.
	Stores []*StoreEncodingCapabilities `json:"stores,omitempty"`
}

// ComputeClusterStatus computes the cluster-wide encoding status from
// individual store capabilities.
func ComputeClusterStatus(stores []*StoreEncodingCapabilities, totalTiFlash int) *ClusterEncodingStatus {
	status := &ClusterEncodingStatus{
		TotalTiFlashStores:    totalTiFlash,
		EncodingEnabledStores: len(stores),
		AllStoresReady:        len(stores) == totalTiFlash && totalTiFlash > 0,
		MinEncodingVersion:    0,
		Stores:                stores,
	}

	if len(stores) > 0 {
		status.MinEncodingVersion = stores[0].EncodingVersion
		for _, s := range stores[1:] {
			if s.EncodingVersion < status.MinEncodingVersion {
				status.MinEncodingVersion = s.EncodingVersion
			}
		}
	}

	return status
}

// NewFullEncodingCapabilities creates capabilities for a store that supports
// all 4 phases of encoded operations.
func NewFullEncodingCapabilities(storeID uint64) *StoreEncodingCapabilities {
	return &StoreEncodingCapabilities{
		StoreID: storeID,
		Capabilities: []Capability{
			CapDictionaryEncoding,
			CapEncodedFilter,
			CapEncodedGroupBy,
			CapEncodedBloomFilter,
			CapEncodedStarJoin,
		},
		MaxDictCardinality: 4096,
		EncodingVersion:    3,
	}
}

// PhaseCapabilities maps encoding versions to their supported capabilities.
var PhaseCapabilities = map[uint32][]Capability{
	1: {CapDictionaryEncoding},
	2: {CapDictionaryEncoding, CapEncodedFilter, CapEncodedGroupBy},
	3: {CapDictionaryEncoding, CapEncodedFilter, CapEncodedGroupBy, CapEncodedBloomFilter, CapEncodedStarJoin},
}

// SupportsPhase checks if a store supports a specific encoding phase.
func (s *StoreEncodingCapabilities) SupportsPhase(phase uint32) bool {
	return s.EncodingVersion >= phase
}

// ClusterSupportsPhase checks if all stores in the cluster support a given phase.
func (status *ClusterEncodingStatus) ClusterSupportsPhase(phase uint32) bool {
	return status.AllStoresReady && status.MinEncodingVersion >= phase
}

// OperationFeasibility describes whether a specific encoded operation can run
// across the cluster.
type OperationFeasibility struct {
	// Operation is the capability being checked
	Operation Capability `json:"operation"`

	// Feasible indicates all relevant stores support this operation
	Feasible bool `json:"feasible"`

	// StoresReady is the count of stores that support this operation
	StoresReady int `json:"stores_ready"`

	// StoresTotal is the total number of TiFlash stores
	StoresTotal int `json:"stores_total"`
}

// CheckOperationFeasibility checks if a specific encoded operation can run
// on all TiFlash stores in the cluster.
func CheckOperationFeasibility(stores []*StoreEncodingCapabilities, totalTiFlash int, op Capability) *OperationFeasibility {
	result := &OperationFeasibility{
		Operation:   op,
		StoresTotal: totalTiFlash,
	}

	for _, s := range stores {
		if s.HasCapability(op) {
			result.StoresReady++
		}
	}

	result.Feasible = result.StoresReady == totalTiFlash && totalTiFlash > 0
	return result
}

// AllOperationsFeasibility checks feasibility for all encoded operations.
func AllOperationsFeasibility(stores []*StoreEncodingCapabilities, totalTiFlash int) []*OperationFeasibility {
	ops := []Capability{
		CapDictionaryEncoding,
		CapEncodedFilter,
		CapEncodedGroupBy,
		CapEncodedBloomFilter,
		CapEncodedStarJoin,
	}

	results := make([]*OperationFeasibility, 0, len(ops))
	for _, op := range ops {
		results = append(results, CheckOperationFeasibility(stores, totalTiFlash, op))
	}
	return results
}
