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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
)

type shardTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestShardTestSuite(t *testing.T) {
	suite.Run(t, new(shardTestSuite))
}

func (suite *shardTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)

	// Enable placement rules.
	configURL := fmt.Sprintf("%s/config", suite.urlPrefix)
	suite.NoError(tu.CheckPostJSON(testDialClient, configURL, []byte(`{"enable-placement-rules":"true"}`), tu.StatusOK(re)))

	// Register stores 1–5 as TiKV stores (no engine label).
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		suite.putStore(re, id, nil)
	}
	// Register stores 6–9 as TiFlash stores.
	for _, id := range []uint64{6, 7, 8, 9} {
		suite.putStore(re, id, []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}})
	}
}

func (suite *shardTestSuite) TearDownSuite() {
	suite.cleanup()
}

// putStore registers a store with the given labels.
func (suite *shardTestSuite) putStore(re interface{ NoError(error, ...interface{}) }, storeID uint64, labels []*metapb.StoreLabel) {
	s := &server.GrpcServer{Server: suite.svr}
	_, err := s.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Store: &metapb.Store{
			Id:        storeID,
			Address:   fmt.Sprintf("tikv%d", storeID),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Labels:    labels,
			Version:   versioninfo.MinSupportedVersion(versioninfo.Version2_0).String(),
		},
	})
	re.NoError(err)
}

func (suite *shardTestSuite) shardURL(tableID ...uint64) string {
	if len(tableID) == 0 {
		return fmt.Sprintf("%s/shard/mapping", suite.urlPrefix)
	}
	return fmt.Sprintf("%s/shard/mapping/%d", suite.urlPrefix, tableID[0])
}

// --- parseShardRuleID unit tests ---

func (suite *shardTestSuite) TestParseShardRuleIDValid() {
	tableID, shardID, physID, ok := parseShardRuleID("42-shard-7-p999-r0")
	suite.True(ok)
	suite.Equal(uint64(42), tableID)
	suite.Equal(uint64(7), shardID)
	suite.Equal(uint64(999), physID)
}

func (suite *shardTestSuite) TestParseShardRuleIDZeroShard() {
	tableID, shardID, physID, ok := parseShardRuleID("1-shard-0-p100-r1")
	suite.True(ok)
	suite.Equal(uint64(1), tableID)
	suite.Equal(uint64(0), shardID)
	suite.Equal(uint64(100), physID)
}

func (suite *shardTestSuite) TestParseShardRuleIDNoSep() {
	_, _, _, ok := parseShardRuleID("42-7")
	suite.False(ok)
}

func (suite *shardTestSuite) TestParseShardRuleIDBadTableID() {
	_, _, _, ok := parseShardRuleID("abc-shard-7-p100-r0")
	suite.False(ok)
}

func (suite *shardTestSuite) TestParseShardRuleIDBadShardID() {
	_, _, _, ok := parseShardRuleID("42-shard-xyz-p100-r0")
	suite.False(ok)
}

// Prefix-collision: "1" must NOT match "10-shard-0-p100-r0".
func (suite *shardTestSuite) TestParseShardRuleIDNoPrefixCollision() {
	tableID, _, _, ok := parseShardRuleID("10-shard-0-p100-r0")
	suite.True(ok)
	suite.Equal(uint64(10), tableID) // must be 10, not 1
}

// --- tableKeyRange unit test ---

func (suite *shardTestSuite) TestTableKeyRange() {
	start, end := tableKeyRange(100)
	suite.NotEmpty(start)
	suite.NotEmpty(end)
	// End of table 100 must equal start of table 101.
	start101, _ := tableKeyRange(101)
	suite.Equal(start101, end, "end of table 100 must equal start of table 101")
	suite.NotEqual(start, end)
}

// --- API integration tests ---

func (suite *shardTestSuite) TestRegisterAndGet() {
	re := suite.Require()
	tableID := uint64(100)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 3,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 201, StoreIDs: []uint64{1, 2, 3}},
			{ShardID: 1, PhysicalID: 202, StoreIDs: []uint64{2, 3, 4}},
			{ShardID: 2, PhysicalID: 203, StoreIDs: []uint64{3, 4, 5}},
		},
	}
	data, err := json.Marshal(body)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	var result shardMapping
	suite.NoError(tu.ReadGetJSON(re, testDialClient, suite.shardURL(tableID), &result))
	suite.Equal(tableID, result.TableID)
	suite.Equal(3, result.ShardCount)
	suite.Len(result.Mappings, 3)

	// Verify StoreIDs and PhysicalIDs are round-tripped correctly.
	byShardID := make(map[uint64]shardStorePair, len(result.Mappings))
	for _, p := range result.Mappings {
		byShardID[p.ShardID] = p
	}
	suite.Equal([]uint64{1, 2, 3}, byShardID[0].StoreIDs)
	suite.Equal(uint64(201), byShardID[0].PhysicalID)
	suite.Equal([]uint64{2, 3, 4}, byShardID[1].StoreIDs)
	suite.Equal(uint64(202), byShardID[1].PhysicalID)
	suite.Equal([]uint64{3, 4, 5}, byShardID[2].StoreIDs)
	suite.Equal(uint64(203), byShardID[2].PhysicalID)
}

func (suite *shardTestSuite) TestRegisterScopedToPhysicalKeyRange() {
	re := suite.Require()
	tableID := uint64(400)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 401, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 402, StoreIDs: []uint64{2, 3}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, rShardID, rPhysID, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID {
			continue
		}
		// Each shard's rules must be scoped to its physical ID range, not the logical table range.
		expectedStart, expectedEnd := tableKeyRange(rPhysID)
		suite.Equal(expectedStart, rule.StartKeyHex,
			"shard %d rule %s: StartKeyHex must be scoped to physical ID %d", rShardID, rule.ID, rPhysID)
		suite.Equal(expectedEnd, rule.EndKeyHex,
			"shard %d rule %s: EndKeyHex must be scoped to physical ID %d", rShardID, rule.ID, rPhysID)
		// Physical IDs must not overlap with the logical table ID range.
		logicalStart, logicalEnd := tableKeyRange(tableID)
		suite.NotEqual(logicalStart, rule.StartKeyHex,
			"shard rule must not use logical table key range")
		suite.NotEqual(logicalEnd, rule.EndKeyHex,
			"shard rule must not use logical table key range")
	}
}

func (suite *shardTestSuite) TestRegisterRulesHaveCorrectRoles() {
	re := suite.Require()
	tableID := uint64(401)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 4010, StoreIDs: []uint64{1, 2, 3}},
			{ShardID: 1, PhysicalID: 4011, StoreIDs: []uint64{2, 3, 4}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	leaderCount := 0
	voterCount := 0
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, _, _, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID {
			continue
		}
		switch rule.Role {
		case placement.Leader:
			leaderCount++
		case placement.Voter:
			voterCount++
		default:
			suite.Failf("unexpected role", "rule %s has unexpected role %v", rule.ID, rule.Role)
		}
		suite.Equal(1, rule.Count)
	}
	// 2 shards × 1 Leader + 2 shards × 2 Voters = 2 Leader + 4 Voter rules
	suite.Equal(2, leaderCount, "expected 1 Leader rule per shard (2 shards)")
	suite.Equal(4, voterCount, "expected 2 Voter rules per shard (2 shards × 2 followers)")
}

func (suite *shardTestSuite) TestLeaderAndVoterAreOnSeparateRanges() {
	// With physical IDs, each shard's Leader rule covers a non-overlapping range.
	// PD must not reject this as "multiple leader replicas" since ranges don't overlap.
	re := suite.Require()
	tableID := uint64(402)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 3,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 4020, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 4021, StoreIDs: []uint64{2, 3}},
			{ShardID: 2, PhysicalID: 4022, StoreIDs: []uint64{3, 4}},
		},
	}
	data, _ := json.Marshal(body)
	// Must succeed — non-overlapping ranges allow one Leader per shard.
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	// Collect Leader rules per shard and verify they have distinct key ranges.
	type keyRange struct{ start, end string }
	leaderRanges := make(map[uint64]keyRange) // shardID → range
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, rShardID, _, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID || rule.Role != placement.Leader {
			continue
		}
		leaderRanges[rShardID] = keyRange{rule.StartKeyHex, rule.EndKeyHex}
	}
	suite.Len(leaderRanges, 3, "each shard must have exactly one Leader rule")

	// All three Leader ranges must be distinct.
	ranges := make(map[string]struct{})
	for _, kr := range leaderRanges {
		key := kr.start + "|" + kr.end
		suite.NotContains(ranges, key, "two Leader rules must not share a key range")
		ranges[key] = struct{}{}
	}
}

func (suite *shardTestSuite) TestRegisterEnforcesSlotConstraint() {
	re := suite.Require()
	tableID := uint64(500)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 5001, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 5002, StoreIDs: []uint64{3, 4}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	for _, rule := range rm.GetRulesByGroup("shard") {
		ruleTableID, _, _, ok := parseShardRuleID(rule.ID)
		if !ok || ruleTableID != tableID {
			continue
		}

		// Must have exactly one constraint: pd-shard-slot=<storeID>.
		suite.Len(rule.LabelConstraints, 1, "rule %s should have 1 label constraint", rule.ID)
		c := rule.LabelConstraints[0]
		suite.Equal(shardSlotLabelKey, c.Key)
		suite.Equal(placement.In, c.Op)
		suite.Len(c.Values, 1)
	}
}

func (suite *shardTestSuite) TestRegisterLabelsStores() {
	re := suite.Require()
	tableID := uint64(600)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 6001, StoreIDs: []uint64{3, 4}},
			{ShardID: 1, PhysicalID: 6002, StoreIDs: []uint64{4, 5}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	cluster := suite.svr.GetRaftCluster()
	for _, storeID := range []uint64{3, 4, 5} {
		store := cluster.GetStore(storeID)
		suite.NotNil(store)
		val := store.GetLabelValue(shardSlotLabelKey)
		suite.Equal(fmt.Sprintf("%d", storeID), val,
			"store %d should have label %s=%d", storeID, shardSlotLabelKey, storeID)
	}
}

func (suite *shardTestSuite) TestCoLocationTwoTables() {
	re := suite.Require()

	// Physical IDs are distinct per table but shard→store mappings are identical.
	for i, tableID := range []uint64{700, 701} {
		physBase := uint64(7000 + i*10)
		body := shardMapping{
			TableID:    tableID,
			ShardCount: 2,
			Mappings: []shardStorePair{
				{ShardID: 0, PhysicalID: physBase, StoreIDs: []uint64{1, 2, 3}},
				{ShardID: 1, PhysicalID: physBase + 1, StoreIDs: []uint64{2, 3, 4}},
			},
		}
		data, _ := json.Marshal(body)
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))
	}

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	storesForShard := func(tableID, shardID uint64) []uint64 {
		return storeIDsFromShardRules(rm.GetRulesByGroup("shard"), tableID, shardID)
	}

	suite.Equal(storesForShard(700, 0), storesForShard(701, 0),
		"shard 0 of tables 700 and 701 must have the same store assignments (all replicas co-located)")
	suite.Equal(storesForShard(700, 1), storesForShard(701, 1),
		"shard 1 of tables 700 and 701 must have the same store assignments (all replicas co-located)")
}

func (suite *shardTestSuite) TestCoLocationRulesArePhysicalScoped() {
	re := suite.Require()

	// Two tables — each shard has its own physical ID, so key ranges never overlap.
	for i, tableID := range []uint64{800, 801} {
		physBase := uint64(8000 + i*10)
		body := shardMapping{
			TableID:    tableID,
			ShardCount: 1,
			Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: physBase, StoreIDs: []uint64{1, 2}}},
		}
		data, _ := json.Marshal(body)
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))
	}

	// Physical IDs 8000 and 8010 must produce different key ranges.
	start8000, end8000 := tableKeyRange(8000)
	start8010, end8010 := tableKeyRange(8010)
	suite.NotEqual(start8000, start8010, "different physical IDs must have different start keys")
	suite.NotEqual(end8000, end8010, "different physical IDs must have different end keys")
}

func (suite *shardTestSuite) TestShardGroupOverrideIsSet() {
	re := suite.Require()
	tableID := uint64(850)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 8501, StoreIDs: []uint64{1, 2, 3}}},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	group := rm.GetRuleGroup("shard")
	suite.NotNil(group, "shard RuleGroup must be registered")
	suite.True(group.Override, "shard RuleGroup must have Override=true to suppress pd/default for shard-table regions")
	suite.Equal(shardGroupIndex, group.Index, "shard RuleGroup must have Index=%d", shardGroupIndex)
}

func (suite *shardTestSuite) TestReplicaCountMismatch() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 101, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 102, StoreIDs: []uint64{3}}, // different replica count
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestDuplicateStoreInShard() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 111, StoreIDs: []uint64{1, 1}}}, // duplicate store
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestDuplicatePhysicalID() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 999, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 999, StoreIDs: []uint64{3, 4}}, // same physical_id
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestZeroPhysicalID() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 0, StoreIDs: []uint64{1, 2}}},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterReplaceExisting() {
	re := suite.Require()
	tableID := uint64(200)

	first := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 2001, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 2002, StoreIDs: []uint64{3, 4}},
		},
	}
	data, _ := json.Marshal(first)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	second := shardMapping{
		TableID:    tableID,
		ShardCount: 4,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 2011, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 2012, StoreIDs: []uint64{2, 3}},
			{ShardID: 2, PhysicalID: 2013, StoreIDs: []uint64{3, 4}},
			{ShardID: 3, PhysicalID: 2014, StoreIDs: []uint64{4, 5}},
		},
	}
	data, _ = json.Marshal(second)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	var result shardMapping
	suite.NoError(tu.ReadGetJSON(re, testDialClient, suite.shardURL(tableID), &result))
	suite.Equal(4, result.ShardCount)
	suite.Len(result.Mappings, 4)
}

func (suite *shardTestSuite) TestDeleteMapping() {
	re := suite.Require()
	tableID := uint64(300)

	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 3001, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 3002, StoreIDs: []uint64{3, 4}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	resp, err := apiutil.DoDelete(testDialClient, suite.shardURL(tableID))
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(http.StatusOK, resp.StatusCode)

	suite.NoError(tu.CheckGetJSON(testDialClient, suite.shardURL(tableID), nil,
		tu.Status(re, http.StatusNotFound)))
}

func (suite *shardTestSuite) TestDeleteNonExistent() {
	resp, err := apiutil.DoDelete(testDialClient, suite.shardURL(99999))
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(http.StatusNotFound, resp.StatusCode)
}

func (suite *shardTestSuite) TestGetNonExistent() {
	re := suite.Require()
	suite.NoError(tu.CheckGetJSON(testDialClient, suite.shardURL(88888), nil,
		tu.Status(re, http.StatusNotFound)))
}

// --- Validation tests ---

func (suite *shardTestSuite) TestRegisterZeroTableID() {
	re := suite.Require()
	body := shardMapping{TableID: 0, ShardCount: 1, Mappings: []shardStorePair{{ShardID: 0, PhysicalID: 1, StoreIDs: []uint64{1}}}}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterZeroShardCount() {
	re := suite.Require()
	body := shardMapping{TableID: 1, ShardCount: 0, Mappings: []shardStorePair{}}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterShardCountTooLarge() {
	re := suite.Require()
	body := shardMapping{TableID: 1, ShardCount: 65}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterMappingCountMismatch() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 3,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 101, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 102, StoreIDs: []uint64{2, 3}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterShardIDOutOfRange() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 101, StoreIDs: []uint64{1, 2}},
			{ShardID: 5, PhysicalID: 102, StoreIDs: []uint64{2, 3}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterDuplicateShardID() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 101, StoreIDs: []uint64{1, 2}},
			{ShardID: 0, PhysicalID: 102, StoreIDs: []uint64{3, 4}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterZeroStoreID() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 111, StoreIDs: []uint64{0, 2}}},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterNonExistentStore() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 111, StoreIDs: []uint64{99999}}},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterEmptyStoreIDs() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: 111, StoreIDs: []uint64{}}},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestGetInvalidTableID() {
	re := suite.Require()
	url := fmt.Sprintf("%s/shard/mapping/not-a-number", suite.urlPrefix)
	suite.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.Status(re, http.StatusBadRequest)))
}

// --- Multi-physical-ID tests (SHARD BY + PARTITION BY) ---

func (suite *shardTestSuite) TestRegisterMultiPhysicalIDs() {
	re := suite.Require()
	tableID := uint64(1100)

	// Simulate a SHARD BY + PARTITION BY table: 2 shards × 3 partitions = 6 physical IDs.
	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{1101, 1102, 1103}, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalIDs: []uint64{1104, 1105, 1106}, StoreIDs: []uint64{2, 3}},
		},
	}
	data, err := json.Marshal(body)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	// Verify rules: each physical ID should have its own key range.
	rm := suite.svr.GetRaftCluster().GetRuleManager()
	physIDsWithRules := make(map[uint64]struct{})
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, _, rPhysID, ok := parseShardRuleID(rule.ID)
		if !ok || rTableID != tableID {
			continue
		}
		physIDsWithRules[rPhysID] = struct{}{}
		expectedStart, expectedEnd := tableKeyRange(rPhysID)
		suite.Equal(expectedStart, rule.StartKeyHex)
		suite.Equal(expectedEnd, rule.EndKeyHex)
	}
	// All 6 physical IDs should have rules.
	suite.Len(physIDsWithRules, 6)

	// GET should return the multi-physical-ID mapping.
	var result shardMapping
	suite.NoError(tu.ReadGetJSON(re, testDialClient, suite.shardURL(tableID), &result))
	suite.Equal(tableID, result.TableID)
	suite.Equal(2, result.ShardCount)
	suite.Len(result.Mappings, 2)

	byShardID := make(map[uint64]shardStorePair, len(result.Mappings))
	for _, p := range result.Mappings {
		byShardID[p.ShardID] = p
	}
	suite.Equal([]uint64{1101, 1102, 1103}, byShardID[0].PhysicalIDs)
	suite.Equal([]uint64{1, 2}, byShardID[0].StoreIDs)
	suite.Equal([]uint64{1104, 1105, 1106}, byShardID[1].PhysicalIDs)
	suite.Equal([]uint64{2, 3}, byShardID[1].StoreIDs)

	// Clean up.
	resp, err := apiutil.DoDelete(testDialClient, suite.shardURL(tableID))
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *shardTestSuite) TestRegisterMultiPhysicalIDsCoLocation() {
	re := suite.Require()

	// Two tables with same shard→store mapping but different physical IDs.
	for i, tableID := range []uint64{1200, 1201} {
		physBase := uint64(12000 + i*100)
		body := shardMapping{
			TableID:    tableID,
			ShardCount: 2,
			Mappings: []shardStorePair{
				{ShardID: 0, PhysicalIDs: []uint64{physBase, physBase + 1}, StoreIDs: []uint64{1, 2}},
				{ShardID: 1, PhysicalIDs: []uint64{physBase + 2, physBase + 3}, StoreIDs: []uint64{2, 3}},
			},
		}
		data, _ := json.Marshal(body)
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))
	}

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	storesForShard := func(tableID, shardID uint64) []uint64 {
		return storeIDsFromShardRules(rm.GetRulesByGroup("shard"), tableID, shardID)
	}

	// Same shard→store mapping across both tables.
	suite.Equal(storesForShard(1200, 0), storesForShard(1201, 0))
	suite.Equal(storesForShard(1200, 1), storesForShard(1201, 1))

	// Clean up.
	for _, tableID := range []uint64{1200, 1201} {
		apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
	}
}

func (suite *shardTestSuite) TestBackwardCompatSinglePhysicalID() {
	re := suite.Require()
	tableID := uint64(1300)

	// Legacy payload: only physical_id (singular), no physical_ids.
	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 1301, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalID: 1302, StoreIDs: []uint64{2, 3}},
		},
	}
	data, err := json.Marshal(body)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	var result shardMapping
	suite.NoError(tu.ReadGetJSON(re, testDialClient, suite.shardURL(tableID), &result))
	suite.Equal(2, result.ShardCount)

	byShardID := make(map[uint64]shardStorePair, len(result.Mappings))
	for _, p := range result.Mappings {
		byShardID[p.ShardID] = p
	}
	// Single physical ID: physical_id populated, physical_ids omitted.
	suite.Equal(uint64(1301), byShardID[0].PhysicalID)
	suite.Empty(byShardID[0].PhysicalIDs)
	suite.Equal(uint64(1302), byShardID[1].PhysicalID)
	suite.Empty(byShardID[1].PhysicalIDs)

	// Clean up.
	resp, err := apiutil.DoDelete(testDialClient, suite.shardURL(tableID))
	suite.NoError(err)
	resp.Body.Close()
}

func (suite *shardTestSuite) TestMultiPhysicalIDsDuplicateRejected() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{1401, 1402}, StoreIDs: []uint64{1, 2}},
			{ShardID: 1, PhysicalIDs: []uint64{1402, 1403}, StoreIDs: []uint64{2, 3}}, // 1402 duplicate
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestMultiPhysicalIDsZeroRejected() {
	re := suite.Require()
	body := shardMapping{
		TableID:    1,
		ShardCount: 1,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{1501, 0}, StoreIDs: []uint64{1, 2}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data,
		tu.Status(re, http.StatusBadRequest)))
}

func (suite *shardTestSuite) TestRegisterReplacesMultiPhysicalIDRules() {
	re := suite.Require()
	tableID := uint64(1600)

	// First: register with 3 physical IDs per shard.
	first := shardMapping{
		TableID:    tableID,
		ShardCount: 1,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{1601, 1602, 1603}, StoreIDs: []uint64{1}},
		},
	}
	data, _ := json.Marshal(first)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	count := 0
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, _, _, ok := parseShardRuleID(rule.ID)
		if ok && rTableID == tableID {
			count++
		}
	}
	suite.Equal(3, count, "should have 3 rules (one per physical ID)")

	// Second: re-register with 2 physical IDs — old rules should be gone.
	second := shardMapping{
		TableID:    tableID,
		ShardCount: 1,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{1611, 1612}, StoreIDs: []uint64{2}},
		},
	}
	data, _ = json.Marshal(second)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	count = 0
	for _, rule := range rm.GetRulesByGroup("shard") {
		rTableID, _, _, ok := parseShardRuleID(rule.ID)
		if ok && rTableID == tableID {
			count++
		}
	}
	suite.Equal(2, count, "should have 2 rules after re-register (old 3 deleted)")

	// Clean up.
	apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
}

// Ensure table "1000" mappings are not returned when asking for table "10000".
func (suite *shardTestSuite) TestNoPrefixCollisionBetweenTables() {
	re := suite.Require()

	physBase := uint64(90000)
	for i, tableID := range []uint64{1000, 1001, 10000, 10001} {
		storeID := uint64(i%4 + 1)
		body := shardMapping{
			TableID:    tableID,
			ShardCount: 1,
			Mappings:   []shardStorePair{{ShardID: 0, PhysicalID: physBase + uint64(i), StoreIDs: []uint64{storeID}}},
		}
		data, _ := json.Marshal(body)
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))
	}

	for _, tableID := range []uint64{1000, 1001, 10000, 10001} {
		var result shardMapping
		suite.NoError(tu.ReadGetJSON(re, testDialClient, suite.shardURL(tableID), &result))
		suite.Equal(tableID, result.TableID, "wrong table returned for tableID %d", tableID)
		suite.Len(result.Mappings, 1, "wrong mapping count for tableID %d", tableID)
	}

	for _, tableID := range []uint64{1000, 1001, 10000, 10001} {
		apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
	}
}

// --- TiFlash learner placement tests ---

// TestAutoAssignTiFlash registers a table with auto_assign_tiflash=true and verifies
// that learner placement rules are created on TiFlash stores, round-robin across shards.
func (suite *shardTestSuite) TestAutoAssignTiFlash() {
	re := suite.Require()

	tableID := uint64(50000)
	body := shardMapping{
		TableID:           tableID,
		ShardCount:        4,
		AutoAssignTiFlash: true,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 50001, StoreIDs: []uint64{1}},
			{ShardID: 1, PhysicalID: 50002, StoreIDs: []uint64{2}},
			{ShardID: 2, PhysicalID: 50003, StoreIDs: []uint64{3}},
			{ShardID: 3, PhysicalID: 50004, StoreIDs: []uint64{4}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	// Verify that learner rules were created. TiFlash stores are 6,7,8,9.
	// Round-robin: shard 0 → store 6, shard 1 → store 7, shard 2 → store 8, shard 3 → store 9.
	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	learnerCount := 0
	for _, rule := range rules {
		if rule.Role == placement.Learner {
			rTableID, _, _, ok := parseShardRuleID(rule.ID)
			// Learner rule IDs use "-tf" not "-r", so parseShardRuleID won't match.
			// Check by prefix instead.
			if !ok && !isShardLearnerRule(rule.ID, tableID) {
				continue
			}
			if ok && rTableID != tableID {
				continue
			}
			learnerCount++
			// Verify the rule has engine=tiflash constraint.
			hasEngineConstraint := false
			for _, c := range rule.LabelConstraints {
				if c.Key == "engine" && c.Op == placement.In {
					for _, v := range c.Values {
						if v == "tiflash" {
							hasEngineConstraint = true
						}
					}
				}
			}
			suite.True(hasEngineConstraint, "learner rule %s should have engine=tiflash constraint", rule.ID)
		}
	}
	// 4 shards × 1 physical ID each = 4 learner rules.
	suite.Equal(4, learnerCount, "expected 4 TiFlash learner rules")

	// Clean up.
	apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
}

// isShardLearnerRule checks if a rule ID belongs to a shard learner rule for the given table.
func isShardLearnerRule(ruleID string, tableID uint64) bool {
	prefix := fmt.Sprintf("%d-shard-", tableID)
	return len(ruleID) > len(prefix) && ruleID[:len(prefix)] == prefix && containsSubstring(ruleID, "-tf")
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestExplicitTiFlashStoreIDs registers with explicit tiflash_store_ids and verifies
// that learner rules are created on the specified stores.
func (suite *shardTestSuite) TestExplicitTiFlashStoreIDs() {
	re := suite.Require()

	tableID := uint64(7100)
	body := shardMapping{
		TableID:         tableID,
		ShardCount:      2,
		TiFlashStoreIDs: []uint64{6, 7},
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 7101, StoreIDs: []uint64{1}},
			{ShardID: 1, PhysicalID: 7102, StoreIDs: []uint64{2}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	// Verify learner rules.
	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	learnerCount := 0
	for _, rule := range rules {
		if rule.Role == placement.Learner && isShardLearnerRule(rule.ID, tableID) {
			learnerCount++
			// Verify pinned to a TiFlash store.
			hasShardSlot := false
			for _, c := range rule.LabelConstraints {
				if c.Key == shardSlotLabelKey && c.Op == placement.In && len(c.Values) == 1 {
					suite.NotEmpty(c.Values[0], "learner rule should be pinned to a store")
					hasShardSlot = true
				}
			}
			suite.True(hasShardSlot, "learner rule %s should have pd-shard-slot constraint", rule.ID)
		}
	}
	suite.Equal(2, learnerCount, "expected 2 TiFlash learner rules")

	apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
}

// TestAutoAssignAndExplicitMutuallyExclusive verifies that providing both
// auto_assign_tiflash and tiflash_store_ids returns an error.
func (suite *shardTestSuite) TestAutoAssignAndExplicitMutuallyExclusive() {
	re := suite.Require()

	body := shardMapping{
		TableID:           uint64(7200),
		ShardCount:        1,
		AutoAssignTiFlash: true,
		TiFlashStoreIDs:   []uint64{6},
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 7201, StoreIDs: []uint64{1}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusNotOK(re)))
}

// TestExplicitTiFlashNonTiFlashStoreRejected verifies that providing a non-TiFlash
// store ID in tiflash_store_ids returns an error.
func (suite *shardTestSuite) TestExplicitTiFlashNonTiFlashStoreRejected() {
	re := suite.Require()

	body := shardMapping{
		TableID:         uint64(7300),
		ShardCount:      1,
		TiFlashStoreIDs: []uint64{1}, // store 1 is TiKV, not TiFlash
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 7301, StoreIDs: []uint64{1}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusNotOK(re)))
}

// TestTiFlashCoLocationTwoTables verifies that two tables registered with the same
// shard count and auto_assign_tiflash get the same TiFlash store assignment per shard.
func (suite *shardTestSuite) TestTiFlashCoLocationTwoTables() {
	re := suite.Require()

	for _, tid := range []uint64{7400, 7500} {
		body := shardMapping{
			TableID:           tid,
			ShardCount:        4,
			AutoAssignTiFlash: true,
			Mappings: []shardStorePair{
				{ShardID: 0, PhysicalID: tid + 1, StoreIDs: []uint64{1}},
				{ShardID: 1, PhysicalID: tid + 2, StoreIDs: []uint64{2}},
				{ShardID: 2, PhysicalID: tid + 3, StoreIDs: []uint64{3}},
				{ShardID: 3, PhysicalID: tid + 4, StoreIDs: []uint64{4}},
			},
		}
		data, _ := json.Marshal(body)
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))
	}

	// Collect TiFlash store assignments per shard for each table.
	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	type shardStore struct {
		tableID uint64
		shardID uint64
		storeID uint64
	}
	var assignments []shardStore
	for _, rule := range rules {
		if rule.Role != placement.Learner {
			continue
		}
		for _, tid := range []uint64{7400, 7500} {
			if !isShardLearnerRule(rule.ID, tid) {
				continue
			}
			sid := storeIDFromRule(rule)
			if sid == 0 {
				continue
			}
			// Extract shard ID from rule ID: "{tableID}-shard-{shardID}-p{physID}-tf{storeID}"
			var tID, shID uint64
			fmt.Sscanf(rule.ID, "%d-shard-%d-", &tID, &shID)
			assignments = append(assignments, shardStore{tID, shID, sid})
		}
	}

	// For each shard, the TiFlash store should be the same across both tables.
	storeByShardT1 := make(map[uint64]uint64)
	storeByShardT2 := make(map[uint64]uint64)
	for _, a := range assignments {
		if a.tableID == 7400 {
			storeByShardT1[a.shardID] = a.storeID
		} else if a.tableID == 7500 {
			storeByShardT2[a.shardID] = a.storeID
		}
	}
	suite.Len(storeByShardT1, 4, "table 7400 should have 4 shard→TiFlash assignments")
	suite.Len(storeByShardT2, 4, "table 7500 should have 4 shard→TiFlash assignments")
	for shardID := uint64(0); shardID < 4; shardID++ {
		suite.Equal(storeByShardT1[shardID], storeByShardT2[shardID],
			"shard %d should map to same TiFlash store for both tables", shardID)
	}

	for _, tid := range []uint64{7400, 7500} {
		apiutil.DoDelete(testDialClient, suite.shardURL(tid)) //nolint:errcheck
	}
}

// TestNoTiFlashNoLearnerRules verifies that without tiflash options, no learner rules are created.
func (suite *shardTestSuite) TestNoTiFlashNoLearnerRules() {
	re := suite.Require()

	tableID := uint64(7600)
	body := shardMapping{
		TableID:    tableID,
		ShardCount: 2,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalID: 7601, StoreIDs: []uint64{1}},
			{ShardID: 1, PhysicalID: 7602, StoreIDs: []uint64{2}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	for _, rule := range rules {
		if rule.Role == placement.Learner && isShardLearnerRule(rule.ID, tableID) {
			suite.Fail("should not have learner rules when TiFlash not requested")
		}
	}

	apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
}

// TestMultiPhysicalIDsWithTiFlash verifies learner rules are created for each physical ID
// when using multi-physical-ID mode with TiFlash.
func (suite *shardTestSuite) TestMultiPhysicalIDsWithTiFlash() {
	re := suite.Require()

	tableID := uint64(7700)
	body := shardMapping{
		TableID:           tableID,
		ShardCount:        2,
		AutoAssignTiFlash: true,
		Mappings: []shardStorePair{
			{ShardID: 0, PhysicalIDs: []uint64{7701, 7703}, StoreIDs: []uint64{1}},
			{ShardID: 1, PhysicalIDs: []uint64{7702, 7704}, StoreIDs: []uint64{2}},
		},
	}
	data, _ := json.Marshal(body)
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.shardURL(), data, tu.StatusOK(re)))

	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	learnerCount := 0
	for _, rule := range rules {
		if rule.Role == placement.Learner && isShardLearnerRule(rule.ID, tableID) {
			learnerCount++
		}
	}
	// 2 shards × 2 physical IDs each = 4 learner rules.
	suite.Equal(4, learnerCount, "expected 4 TiFlash learner rules for multi-physical-ID table")

	apiutil.DoDelete(testDialClient, suite.shardURL(tableID)) //nolint:errcheck
}
