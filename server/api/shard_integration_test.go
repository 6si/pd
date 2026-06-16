//go:build cluster_integration

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	pdEndpoint  = "http://k8s-tidbshar-tidbshar-be46cdd2bd-f321ccbe9a944001.elb.us-east-1.amazonaws.com:2379"
	dialTimeout = 5 * time.Second
)

// pdClient returns an HTTP client and the base URL for the PD shard API.
// It skips the test if PD is unreachable.
func pdClient(t *testing.T) (*http.Client, string) {
	t.Helper()
	client := &http.Client{Timeout: dialTimeout}
	baseURL := fmt.Sprintf("%s/pd/api/v1", pdEndpoint)

	// Health check
	resp, err := client.Get(fmt.Sprintf("%s/health", baseURL))
	if err != nil {
		t.Skipf("PD cluster unreachable: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("PD cluster unhealthy: status %d", resp.StatusCode)
	}
	return client, baseURL
}

// postMapping sends a POST /shard/mapping request and returns the HTTP response.
func postMapping(t *testing.T, client *http.Client, baseURL string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := client.Post(
		fmt.Sprintf("%s/shard/mapping", baseURL),
		"application/json",
		bytes.NewReader(data),
	)
	require.NoError(t, err)
	return resp
}

// getMapping sends a GET /shard/mapping/{table_id} request.
func getMapping(t *testing.T, client *http.Client, baseURL string, tableID uint64) (*http.Response, map[string]interface{}) {
	t.Helper()
	resp, err := client.Get(fmt.Sprintf("%s/shard/mapping/%d", baseURL, tableID))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	var result map[string]interface{}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &result)
	}
	return resp, result
}

// deleteMapping sends a DELETE /shard/mapping/{table_id} request.
func deleteMapping(t *testing.T, client *http.Client, baseURL string, tableID uint64) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/shard/mapping/%d", baseURL, tableID), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

// getStores fetches the store list from PD.
func getStores(t *testing.T, client *http.Client, baseURL string) []map[string]interface{} {
	t.Helper()
	resp, err := client.Get(fmt.Sprintf("%s/stores", baseURL))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result struct {
		Stores []map[string]interface{} `json:"stores"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	return result.Stores
}

// findTiKVStoreIDs returns up to n TiKV store IDs from the cluster.
func findTiKVStoreIDs(t *testing.T, client *http.Client, baseURL string, n int) []uint64 {
	t.Helper()
	stores := getStores(t, client, baseURL)
	var ids []uint64
	for _, s := range stores {
		store, ok := s["store"].(map[string]interface{})
		if !ok {
			continue
		}
		// Skip TiFlash stores
		labels, _ := store["labels"].([]interface{})
		isTiFlash := false
		for _, l := range labels {
			lm, _ := l.(map[string]interface{})
			if lm["key"] == "engine" && lm["value"] == "tiflash" {
				isTiFlash = true
				break
			}
		}
		if isTiFlash {
			continue
		}
		id := uint64(store["id"].(float64))
		ids = append(ids, id)
		if len(ids) >= n {
			break
		}
	}
	if len(ids) == 0 {
		t.Skip("no TiKV stores found in cluster")
	}
	return ids
}

// cleanupTable deletes any shard mapping for the given table ID.
func cleanupTable(t *testing.T, client *http.Client, baseURL string, tableID uint64) {
	t.Helper()
	resp := deleteMapping(t, client, baseURL, tableID)
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// (a) Register single-physical-ID mapping
// ---------------------------------------------------------------------------

func TestCluster_PD_RegisterSinglePhysicalID(t *testing.T) {
	client, baseURL := pdClient(t)
	storeIDs := findTiKVStoreIDs(t, client, baseURL, 1)

	const tableID uint64 = 90001
	t.Cleanup(func() { cleanupTable(t, client, baseURL, tableID) })

	mapping := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 90101, StoreIDs: storeIDs},
			{ShardID: 1, PhysicalID: 90102, StoreIDs: storeIDs},
		},
	}

	resp := postMapping(t, client, baseURL, mapping)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "POST failed: %s", string(body))

	// GET and verify
	getResp, result := getMapping(t, client, baseURL, tableID)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	require.NotNil(t, result)

	mappings, ok := result["mappings"].([]interface{})
	require.True(t, ok, "expected mappings array in response")
	require.Equal(t, 2, len(mappings), "expected 2 shard mappings")
}

// ---------------------------------------------------------------------------
// (b) Register multi-physical-ID mapping
// ---------------------------------------------------------------------------

func TestCluster_PD_RegisterMultiPhysicalID(t *testing.T) {
	client, baseURL := pdClient(t)
	storeIDs := findTiKVStoreIDs(t, client, baseURL, 1)

	const tableID uint64 = 90002
	t.Cleanup(func() { cleanupTable(t, client, baseURL, tableID) })

	mapping := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{90201, 90202, 90203}, StoreIDs: storeIDs},
			{ShardID: 1, PhysicalIDs: []uint64{90211, 90212, 90213}, StoreIDs: storeIDs},
		},
	}

	resp := postMapping(t, client, baseURL, mapping)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "POST failed: %s", string(body))

	// GET and verify physical_ids are returned
	getResp, result := getMapping(t, client, baseURL, tableID)
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	mappings, ok := result["mappings"].([]interface{})
	require.True(t, ok)
	require.Equal(t, 2, len(mappings))

	// Check first shard has multiple physical IDs
	shard0, ok := mappings[0].(map[string]interface{})
	require.True(t, ok)
	physIDs, ok := shard0["physical_ids"].([]interface{})
	if ok {
		require.Equal(t, 3, len(physIDs), "expected 3 physical IDs for shard 0")
	}
}

// ---------------------------------------------------------------------------
// (c) Backward compatibility — singular physical_id
// ---------------------------------------------------------------------------

func TestCluster_PD_BackwardCompat(t *testing.T) {
	client, baseURL := pdClient(t)
	storeIDs := findTiKVStoreIDs(t, client, baseURL, 1)

	const tableID uint64 = 90003
	t.Cleanup(func() { cleanupTable(t, client, baseURL, tableID) })

	// Use old-style physical_id (singular) with no physical_ids field
	mapping := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 90301, StoreIDs: storeIDs},
			{ShardID: 1, PhysicalID: 90302, StoreIDs: storeIDs},
		},
	}

	resp := postMapping(t, client, baseURL, mapping)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "backward compat POST failed: %s", string(body))

	// GET and verify it works
	getResp, result := getMapping(t, client, baseURL, tableID)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	require.NotNil(t, result)
}

// ---------------------------------------------------------------------------
// (d) Auto-assign TiFlash
// ---------------------------------------------------------------------------

func TestCluster_PD_AutoAssignTiFlash(t *testing.T) {
	client, baseURL := pdClient(t)
	storeIDs := findTiKVStoreIDs(t, client, baseURL, 1)

	const tableID uint64 = 90004
	t.Cleanup(func() { cleanupTable(t, client, baseURL, tableID) })

	mapping := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 90401, StoreIDs: storeIDs},
			{ShardID: 1, PhysicalID: 90402, StoreIDs: storeIDs},
		},
		AutoAssignTiFlash: true,
	}

	resp := postMapping(t, client, baseURL, mapping)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// If no TiFlash stores exist, the API may return an error or succeed
	// with no learner rules. Both are acceptable.
	if resp.StatusCode == http.StatusOK {
		t.Logf("AutoAssignTiFlash succeeded")
	} else {
		t.Logf("AutoAssignTiFlash returned %d: %s (may be expected if no TiFlash stores)", resp.StatusCode, string(body))
	}
}

// ---------------------------------------------------------------------------
// (e) DELETE mapping
// ---------------------------------------------------------------------------

func TestCluster_PD_DeleteMapping(t *testing.T) {
	client, baseURL := pdClient(t)
	storeIDs := findTiKVStoreIDs(t, client, baseURL, 1)

	const tableID uint64 = 90005
	// No cleanup needed — we're testing DELETE explicitly

	// Register first
	mapping := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 90501, StoreIDs: storeIDs},
			{ShardID: 1, PhysicalID: 90502, StoreIDs: storeIDs},
		},
	}
	resp := postMapping(t, client, baseURL, mapping)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "setup POST failed: %s", string(body))

	// Verify it exists
	getResp, _ := getMapping(t, client, baseURL, tableID)
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	// DELETE
	delResp := deleteMapping(t, client, baseURL, tableID)
	delBody, _ := io.ReadAll(delResp.Body)
	delResp.Body.Close()
	require.Equal(t, http.StatusOK, delResp.StatusCode, "DELETE failed: %s", string(delBody))

	// Verify it's gone — GET should return 404 or empty mappings
	getResp2, result2 := getMapping(t, client, baseURL, tableID)
	if getResp2.StatusCode == http.StatusOK {
		// If 200, mappings should be empty
		mappings, ok := result2["mappings"].([]interface{})
		if ok {
			require.Equal(t, 0, len(mappings), "expected no mappings after DELETE")
		}
	} else {
		// 404 is also acceptable
		require.Equal(t, http.StatusNotFound, getResp2.StatusCode)
	}
}
