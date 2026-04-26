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

package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// shardSlotLabelKey is the store label used to pin a store to a shard slot.
// A store is labelled pd-shard-slot=<storeID>, and each shard placement rule
// carries a matching LabelConstraint so PD's rule checker enforces co-location.
const shardSlotLabelKey = "pd-shard-slot"

// shardGroupIndex is the RuleGroup index for the shard group. It must be higher
// than 0 (the pd/default group's index) so that when Override is true the shard
// rules take precedence and suppress pd/default for shard-table key ranges.
const shardGroupIndex = 1

type shardHandler struct {
	svr *server.Server
	rd  *render.Render
}

// shardMapping is the JSON body for POST /shard/mapping and the response for
// GET /shard/mapping/{table_id}.
//
// Each element of Mappings covers one shard slot. StoreIDs[0] is the preferred
// Raft leader store; StoreIDs[1:] are Voter (follower) stores. All shards in a
// mapping must have the same len(StoreIDs). Two tables registered with the same
// shard→store assignment will have all N replicas co-located per shard across
// both tables.
type shardMapping struct {
	TableID    uint64           `json:"table_id"`
	ShardCount int              `json:"shard_count"`
	Mappings   []shardStorePair `json:"mappings"`
}

// shardStorePair assigns a shard slot to one or more stores.
// PhysicalID is the TiDB physical table ID allocated for this shard at CREATE
// TABLE time. It defines the shard's key range [t{PhysicalID}, t{PhysicalID+1}),
// giving each shard a non-overlapping range so PD can pin one Leader per shard.
// StoreIDs[0] receives the Leader placement rule; StoreIDs[1:] receive Voter rules.
type shardStorePair struct {
	ShardID    uint64   `json:"shard_id"`
	PhysicalID uint64   `json:"physical_id"`
	StoreIDs   []uint64 `json:"store_ids"`
}

func newShardHandler(svr *server.Server, rd *render.Render) *shardHandler {
	return &shardHandler{
		svr: svr,
		rd:  rd,
	}
}

// shardRuleID returns the canonical rule ID for a (tableID, shardID, physicalID, replicaIdx) tuple.
// Format: "{tableID}-shard-{shardID}-p{physicalID}-r{replicaIdx}".
// replicaIdx 0 is the Leader rule; replicaIdx 1+ are Voter rules.
func shardRuleID(tableID, shardID, physicalID uint64, replicaIdx int) string {
	return fmt.Sprintf("%d-shard-%d-p%d-r%d", tableID, shardID, physicalID, replicaIdx)
}

// parseShardRuleID parses a rule ID produced by shardRuleID and returns
// (tableID, shardID, physicalID, ok). The replicaIdx suffix is accepted but
// not returned — callers only need tableID/shardID/physicalID for grouping.
func parseShardRuleID(id string) (uint64, uint64, uint64, bool) {
	const shardSep = "-shard-"
	idx := strings.Index(id, shardSep)
	if idx < 0 {
		return 0, 0, 0, false
	}
	tableID, err := strconv.ParseUint(id[:idx], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	// rest = "{shardID}-p{physicalID}-r{replicaIdx}"
	rest := id[idx+len(shardSep):]
	pIdx := strings.Index(rest, "-p")
	if pIdx < 0 {
		return 0, 0, 0, false
	}
	shardID, err := strconv.ParseUint(rest[:pIdx], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	rest = rest[pIdx+2:] // skip "-p"
	rIdx := strings.Index(rest, "-r")
	if rIdx < 0 {
		return 0, 0, 0, false
	}
	physicalID, err := strconv.ParseUint(rest[:rIdx], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	return tableID, shardID, physicalID, true
}

// storeIDsFromShardRules reads all store IDs for a shard slot from the set of rules
// for that (tableID, shardID). Returns nil if no rules are found.
// Index 0 = Leader-rule store; index 1+ = Voter-rule stores.
func storeIDsFromShardRules(rules []*placement.Rule, tableID, shardID uint64) []uint64 {
	type replicaStore struct {
		idx     int
		storeID uint64
	}
	var found []replicaStore
	for _, rule := range rules {
		rTableID, rShardID, _, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID || rShardID != shardID {
			continue
		}
		storeID := storeIDFromRule(rule)
		if storeID == 0 {
			continue
		}
		// Parse replicaIdx from the trailing "-r<N>" in the rule ID.
		rPart := strings.LastIndex(rule.ID, "-r")
		replicaIdx := 0
		if rPart >= 0 {
			if n, err := strconv.Atoi(rule.ID[rPart+2:]); err == nil {
				replicaIdx = n
			}
		}
		found = append(found, replicaStore{replicaIdx, storeID})
	}
	if len(found) == 0 {
		return nil
	}
	// Sort by replicaIdx so index 0 is the Leader store.
	for i := 1; i < len(found); i++ {
		for j := i; j > 0 && found[j].idx < found[j-1].idx; j-- {
			found[j], found[j-1] = found[j-1], found[j]
		}
	}
	ids := make([]uint64, len(found))
	for i, rs := range found {
		ids[i] = rs.storeID
	}
	return ids
}

// storeIDFromRule extracts the StoreID from the pd-shard-slot label constraint on a rule.
// Returns 0 if the constraint is absent.
func storeIDFromRule(rule *placement.Rule) uint64 {
	for _, c := range rule.LabelConstraints {
		if c.Key == shardSlotLabelKey && c.Op == placement.In && len(c.Values) == 1 {
			id, err := strconv.ParseUint(c.Values[0], 10, 64)
			if err == nil {
				return id
			}
		}
	}
	return 0
}

// tableKeyRange returns the hex-encoded start and end keys for a table's full key range.
// Used for both logical table IDs and physical shard IDs — each shard's physical ID
// produces a non-overlapping range [t{physID}, t{physID+1}).
func tableKeyRange(tableID uint64) (startKeyHex, endKeyHex string) {
	start := codec.EncodeBytes(codec.GenerateTableKey(int64(tableID)))
	end := codec.EncodeBytes(codec.GenerateTableKey(int64(tableID) + 1))
	return hex.EncodeToString(start), hex.EncodeToString(end)
}

// ensureShardRuleGroup registers the "shard" RuleGroup with Index=1 and Override=true
// if it is not already set up that way. This causes the shard rules to suppress
// pd/default for all key ranges they cover, giving the shard rules full ownership
// of replication for shard-table regions.
func ensureShardRuleGroup(rm *placement.RuleManager) error {
	existing := rm.GetRuleGroup("shard")
	if existing != nil && existing.Index == shardGroupIndex && existing.Override {
		return nil
	}
	return rm.SetRuleGroup(&placement.RuleGroup{
		ID:       "shard",
		Index:    shardGroupIndex,
		Override: true,
	})
}

// @Tags     shard
// @Summary  Register shard-to-store mapping for a table.
// @Description  Registers placement rules for each shard of a table. Each shard's
//
//	PhysicalID defines a non-overlapping key range [t{PhysicalID}, t{PhysicalID+1}).
//	Within that range, StoreIDs[0] receives a Leader rule and StoreIDs[1:] receive
//	Voter rules. Because each shard's key range is non-overlapping, PD can enforce
//	exactly one Leader per shard without checkApplyRules conflicts.
//	The shard RuleGroup uses Override=true to suppress pd/default for shard-table
//	regions. Two tables registered with identical shard→store mappings will have
//	all replicas co-located per shard slot. On failover, Raft elects a Voter from
//	the same shard-assigned stores.
//
// @Accept   json
// @Param    body  body  shardMapping  true  "Shard mapping"
// @Produce  json
// @Success  200  {string}  string  "Shard mapping registered successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /shard/mapping [post]
func (h *shardHandler) Register(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	if !cluster.GetOpts().IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errs.ErrPlacementDisabled.Error())
		return
	}

	var mapping shardMapping
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &mapping); err != nil {
		return
	}

	if mapping.TableID == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "table_id must be non-zero")
		return
	}
	if mapping.ShardCount <= 0 || mapping.ShardCount > 64 {
		h.rd.JSON(w, http.StatusBadRequest, "shard_count must be between 1 and 64")
		return
	}
	if len(mapping.Mappings) != mapping.ShardCount {
		h.rd.JSON(w, http.StatusBadRequest, "number of mappings must equal shard_count")
		return
	}

	// Validate replica count consistency: all shards must have the same number of store IDs.
	replicaCount := -1
	for _, pair := range mapping.Mappings {
		if len(pair.StoreIDs) == 0 {
			h.rd.JSON(w, http.StatusBadRequest,
				fmt.Sprintf("shard %d: store_ids must be non-empty", pair.ShardID))
			return
		}
		if replicaCount == -1 {
			replicaCount = len(pair.StoreIDs)
		} else if len(pair.StoreIDs) != replicaCount {
			h.rd.JSON(w, http.StatusBadRequest,
				"all shards must have the same number of store_ids (replica count)")
			return
		}
	}

	// Validate each entry: shard ID in-range, physical_id non-zero, no duplicates,
	// all stores exist, no duplicate stores per shard.
	seenShards := make(map[uint64]struct{}, len(mapping.Mappings))
	seenPhysIDs := make(map[uint64]struct{}, len(mapping.Mappings))
	seenStores := make(map[uint64]struct{})
	for _, pair := range mapping.Mappings {
		if pair.ShardID >= uint64(mapping.ShardCount) {
			h.rd.JSON(w, http.StatusBadRequest,
				fmt.Sprintf("shard_id %d out of range [0, %d)", pair.ShardID, mapping.ShardCount))
			return
		}
		if _, dup := seenShards[pair.ShardID]; dup {
			h.rd.JSON(w, http.StatusBadRequest,
				fmt.Sprintf("duplicate shard_id %d", pair.ShardID))
			return
		}
		seenShards[pair.ShardID] = struct{}{}

		if pair.PhysicalID == 0 {
			h.rd.JSON(w, http.StatusBadRequest,
				fmt.Sprintf("shard %d: physical_id must be non-zero", pair.ShardID))
			return
		}
		if _, dup := seenPhysIDs[pair.PhysicalID]; dup {
			h.rd.JSON(w, http.StatusBadRequest,
				fmt.Sprintf("shard %d: duplicate physical_id %d", pair.ShardID, pair.PhysicalID))
			return
		}
		seenPhysIDs[pair.PhysicalID] = struct{}{}

		seenInShard := make(map[uint64]struct{}, len(pair.StoreIDs))
		for _, storeID := range pair.StoreIDs {
			if storeID == 0 {
				h.rd.JSON(w, http.StatusBadRequest,
					fmt.Sprintf("shard %d: store_id must be non-zero", pair.ShardID))
				return
			}
			if cluster.GetStore(storeID) == nil {
				h.rd.JSON(w, http.StatusBadRequest,
					fmt.Sprintf("store %d does not exist", storeID))
				return
			}
			if _, dup := seenInShard[storeID]; dup {
				h.rd.JSON(w, http.StatusBadRequest,
					fmt.Sprintf("shard %d: duplicate store_id %d", pair.ShardID, storeID))
				return
			}
			seenInShard[storeID] = struct{}{}
			seenStores[storeID] = struct{}{}
		}
	}

	// Label each target store with pd-shard-slot=<storeID>.
	// Idempotent: the label value equals the store's own ID.
	for storeID := range seenStores {
		labels := []*metapb.StoreLabel{
			{Key: shardSlotLabelKey, Value: strconv.FormatUint(storeID, 10)},
		}
		if err := cluster.UpdateStoreLabels(storeID, labels, false); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to label store %d: %s", storeID, err.Error()))
			return
		}
	}

	// Ensure the shard RuleGroup is set up with Override=true so it suppresses
	// pd/default for shard-table key ranges.
	rm := cluster.GetRuleManager()
	if err := ensureShardRuleGroup(rm); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError,
			fmt.Sprintf("failed to configure shard rule group: %s", err.Error()))
		return
	}

	// Build an atomic batch: delete old rules for this table, then add new ones.
	oldRules := rm.GetRulesByGroup("shard")
	ops := make([]placement.RuleOp, 0, len(oldRules)+len(mapping.Mappings)*replicaCount)
	for _, rule := range oldRules {
		tableID, _, _, ok := parseShardRuleID(rule.ID)
		if ok && tableID == mapping.TableID {
			ops = append(ops, placement.RuleOp{
				Rule:   rule,
				Action: placement.RuleOpDel,
			})
		}
	}

	for _, pair := range mapping.Mappings {
		// Each shard has its own non-overlapping key range via its physical table ID.
		// This allows PD to assign exactly one Leader rule per shard without
		// checkApplyRules "multiple leader replicas" violations.
		startKeyHex, endKeyHex := tableKeyRange(pair.PhysicalID)
		shardID := pair.ShardID
		shardCount := mapping.ShardCount
		for replicaIdx, storeID := range pair.StoreIDs {
			// replicaIdx 0 is the preferred Leader; all others are Voters.
			role := placement.Voter
			if replicaIdx == 0 {
				role = placement.Leader
			}
			ops = append(ops, placement.RuleOp{
				Rule: &placement.Rule{
					GroupID:     "shard",
					ID:          shardRuleID(mapping.TableID, pair.ShardID, pair.PhysicalID, replicaIdx),
					Index:       int(pair.ShardID)*replicaCount + replicaIdx,
					StartKeyHex: startKeyHex,
					EndKeyHex:   endKeyHex,
					Role:        role,
					Count:       1,
					// Pin to the specific store for co-location.
					LabelConstraints: []placement.LabelConstraint{
						{Key: shardSlotLabelKey, Op: placement.In, Values: []string{strconv.FormatUint(storeID, 10)}},
					},
					ShardID:    &shardID,
					ShardCount: &shardCount,
				},
				Action: placement.RuleOpAdd,
			})
		}
	}

	if err := rm.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).Batch(ops); err != nil {
		if errs.ErrRuleContent.Equal(err) || errs.ErrHexDecodingString.Equal(err) {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	h.rd.JSON(w, http.StatusOK, "Shard mapping registered successfully.")
}

// @Tags     shard
// @Summary  Get shard mapping for a table.
// @Param    table_id  path  int  true  "Table ID"
// @Produce  json
// @Success  200  {object}  shardMapping
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  404  {string}  string  "No shard mapping found for the given table."
// @Router   /shard/mapping/{table_id} [get]
func (h *shardHandler) GetMapping(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	if !cluster.GetOpts().IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errs.ErrPlacementDisabled.Error())
		return
	}

	tableIDStr := mux.Vars(r)["table_id"]
	tableID, err := strconv.ParseUint(tableIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	rm := cluster.GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	// Collect physicalID and shardCount per shard for this table.
	type shardMeta struct {
		physicalID uint64
		shardCount int
	}
	metaByShardID := make(map[uint64]shardMeta)
	for _, rule := range rules {
		rTableID, rShardID, rPhysID, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID {
			continue
		}
		sc := 0
		if rule.ShardCount != nil {
			sc = *rule.ShardCount
		}
		metaByShardID[rShardID] = shardMeta{physicalID: rPhysID, shardCount: sc}
	}

	if len(metaByShardID) == 0 {
		h.rd.JSON(w, http.StatusNotFound, errors.Errorf("no shard mapping found for table %d", tableID).Error())
		return
	}

	// Determine ShardCount from the rules metadata.
	var shardCount int
	for _, m := range metaByShardID {
		shardCount = m.shardCount
		break
	}

	pairs := make([]shardStorePair, 0, len(metaByShardID))
	for shardID, meta := range metaByShardID {
		storeIDs := storeIDsFromShardRules(rules, tableID, shardID)
		pairs = append(pairs, shardStorePair{
			ShardID:    shardID,
			PhysicalID: meta.physicalID,
			StoreIDs:   storeIDs,
		})
	}

	// Sort pairs by ShardID for deterministic output.
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j].ShardID < pairs[j-1].ShardID; j-- {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
		}
	}

	h.rd.JSON(w, http.StatusOK, shardMapping{
		TableID:    tableID,
		ShardCount: shardCount,
		Mappings:   pairs,
	})
}

// @Tags     shard
// @Summary  Delete shard mapping for a table.
// @Param    table_id  path  int  true  "Table ID"
// @Produce  json
// @Success  200  {string}  string  "Shard mapping deleted successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  412  {string}  string  "Placement rules feature is disabled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /shard/mapping/{table_id} [delete]
func (h *shardHandler) DeleteMapping(w http.ResponseWriter, r *http.Request) {
	cluster := getCluster(r)
	if !cluster.GetOpts().IsPlacementRulesEnabled() {
		h.rd.JSON(w, http.StatusPreconditionFailed, errs.ErrPlacementDisabled.Error())
		return
	}

	tableIDStr := mux.Vars(r)["table_id"]
	tableID, err := strconv.ParseUint(tableIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	rm := cluster.GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	ops := make([]placement.RuleOp, 0)
	for _, rule := range rules {
		ruleTableID, _, _, ok := parseShardRuleID(rule.ID)
		if ok && ruleTableID == tableID {
			ops = append(ops, placement.RuleOp{
				Rule:   rule,
				Action: placement.RuleOpDel,
			})
		}
	}

	if len(ops) == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("no shard mapping found for table %d", tableID))
		return
	}

	if err := rm.SetKeyType(h.svr.GetConfig().PDServerCfg.KeyType).Batch(ops); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Note: pd-shard-slot store labels are intentionally preserved on deletion.
	// Other tables may still reference the same stores.

	h.rd.JSON(w, http.StatusOK, "Shard mapping deleted successfully.")
}
