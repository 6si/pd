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
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/unrolled/render"
)

type shardHandler struct {
	svr *server.Server
	rd  *render.Render
}

type shardMapping struct {
	TableID    uint64           `json:"table_id"`
	ShardCount int              `json:"shard_count"`
	Mappings   []shardStorePair `json:"mappings"`
}

type shardStorePair struct {
	ShardID uint64 `json:"shard_id"`
	StoreID uint64 `json:"store_id"`
}

func newShardHandler(svr *server.Server, rd *render.Render) *shardHandler {
	return &shardHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     shard
// @Summary  Register shard mapping
// @Accept   json
// @Param    body  body  shardMapping  true  "Shard mapping"
// @Produce  json
// @Success  200  {string}  string  "shard mapping registered"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /shard/mapping [post]
func (h *shardHandler) Register(w http.ResponseWriter, r *http.Request) {
	var mapping shardMapping
	if err := json.NewDecoder(r.Body).Decode(&mapping); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if mapping.ShardCount <= 0 {
		h.rd.JSON(w, http.StatusBadRequest, "shard_count must be positive")
		return
	}

	if len(mapping.Mappings) != mapping.ShardCount {
		h.rd.JSON(w, http.StatusBadRequest, "number of mappings must equal shard_count")
		return
	}

	cluster := getCluster(r)
	rm := cluster.GetRuleManager()
	for _, pair := range mapping.Mappings {
		shardID := pair.ShardID
		shardCount := mapping.ShardCount

		rule := &placement.Rule{
			GroupID:    "shard",
			ID:         strconv.FormatUint(mapping.TableID, 10) + "-shard-" + strconv.FormatUint(pair.ShardID, 10),
			Index:      int(pair.ShardID),
			Role:       placement.Voter,
			Count:      1,
			ShardID:    &shardID,
			ShardCount: &shardCount,
		}

		if err := rm.SetRule(rule); err != nil {
			continue
		}
	}

	h.rd.JSON(w, http.StatusOK, "shard mapping registered")
}

// @Tags     shard
// @Summary  Get shard mapping by table ID
// @Param    table_id  path  int  true  "Table ID"
// @Produce  json
// @Success  200  {object}  []placement.Rule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /shard/mapping/{table_id} [get]
func (h *shardHandler) GetMapping(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableIDStr := vars["table_id"]

	_, err := strconv.ParseUint(tableIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	cluster := getCluster(r)
	rm := cluster.GetRuleManager()
	rules := rm.GetRulesByGroup("shard")

	var mappings []shardStorePair
	for _, rule := range rules {
		if len(rule.ID) > len(tableIDStr)+7 && rule.ID[:len(tableIDStr)] == tableIDStr {
			if rule.ShardID != nil {
				mappings = append(mappings, shardStorePair{
					ShardID: *rule.ShardID,
					StoreID: 0,
				})
			}
		}
	}

	h.rd.JSON(w, http.StatusOK, mappings)
}
