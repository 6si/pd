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
	"encoding/json"
	"net/http"
	"sync"
)

// Registry tracks encoding capabilities reported by TiFlash stores.
// It is safe for concurrent access.
type Registry struct {
	mu    sync.RWMutex
	store map[uint64]*StoreEncodingCapabilities
}

// NewRegistry creates an empty encoding capabilities registry.
func NewRegistry() *Registry {
	return &Registry{
		store: make(map[uint64]*StoreEncodingCapabilities),
	}
}

// Register adds or updates encoding capabilities for a store.
func (r *Registry) Register(caps *StoreEncodingCapabilities) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[caps.StoreID] = caps
}

// Unregister removes encoding capabilities for a store.
func (r *Registry) Unregister(storeID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.store, storeID)
}

// Get retrieves encoding capabilities for a specific store.
func (r *Registry) Get(storeID uint64) (*StoreEncodingCapabilities, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	caps, ok := r.store[storeID]
	return caps, ok
}

// GetAll returns all registered store capabilities.
func (r *Registry) GetAll() []*StoreEncodingCapabilities {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*StoreEncodingCapabilities, 0, len(r.store))
	for _, caps := range r.store {
		result = append(result, caps)
	}
	return result
}

// GetClusterStatus computes the cluster-wide encoding status.
func (r *Registry) GetClusterStatus(totalTiFlashStores int) *ClusterEncodingStatus {
	stores := r.GetAll()
	return ComputeClusterStatus(stores, totalTiFlashStores)
}

// Handler provides HTTP API endpoints for encoding capabilities.
type Handler struct {
	registry         *Registry
	getTiFlashCount  func() int
}

// NewHandler creates an API handler for encoding capabilities.
func NewHandler(registry *Registry, getTiFlashCount func() int) *Handler {
	return &Handler{
		registry:        registry,
		getTiFlashCount: getTiFlashCount,
	}
}

// ServeEncodingStatus handles GET /pd/api/v1/encoding/status
func (h *Handler) ServeEncodingStatus(w http.ResponseWriter, r *http.Request) {
	status := h.registry.GetClusterStatus(h.getTiFlashCount())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// ServeRegisterCapabilities handles POST /pd/api/v1/encoding/capabilities
func (h *Handler) ServeRegisterCapabilities(w http.ResponseWriter, r *http.Request) {
	var caps StoreEncodingCapabilities
	if err := json.NewDecoder(r.Body).Decode(&caps); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if caps.StoreID == 0 {
		http.Error(w, "store_id is required", http.StatusBadRequest)
		return
	}

	h.registry.Register(&caps)
	w.WriteHeader(http.StatusOK)
}
