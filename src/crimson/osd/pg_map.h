// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/common/type_helpers.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"

namespace crimson::osd {
class PG;

/**
 * PGShardMapping
 *
 * Maps pgs to shards.
 */
class PGShardMapping {
public:
  /// Returns mapping if present, NULL_CORE otherwise
  core_id_t get_pg_mapping(spg_t pgid) {
    auto iter = pg_to_core.find(pgid);
    ceph_assert(iter == pg_to_core.end() || iter->second != NULL_CORE);
    return iter == pg_to_core.end() ? NULL_CORE : iter->second;
  }

  /// Returns mapping for pgid, creates new one if it doesn't already exist
  core_id_t maybe_create_pg(spg_t pgid) {
    auto [insert_iter, inserted] = pg_to_core.emplace(pgid, NULL_CORE);
    if (!inserted) {
      ceph_assert(insert_iter->second != NULL_CORE);
      return insert_iter->second;
    } else {
      auto iter = core_to_num_pgs.begin();
      auto min_iter = iter++;
      for (; iter != core_to_num_pgs.end(); ++iter) {
	if (min_iter->second > iter->second) min_iter = iter;
      }
      insert_iter->second = min_iter->first;
      min_iter->second++;
      return insert_iter->second;
    }
  }

  /// Remove pgid
  void remove_pg(spg_t pgid) {
    auto iter = pg_to_core.find(pgid);
    ceph_assert(iter != pg_to_core.end());
    ceph_assert(iter->second != NULL_CORE);
    auto count_iter = core_to_num_pgs.find(iter->second);
    ceph_assert(count_iter != core_to_num_pgs.end());
    ceph_assert(count_iter->second > 0);
    --(count_iter->second);
    pg_to_core.erase(iter);
  }

  size_t get_num_pgs() const { return pg_to_core.size(); }

  /// Map to cores in [min_core_mapping, core_mapping_limit)
  PGShardMapping(core_id_t min_core_mapping, core_id_t core_mapping_limit) {
    ceph_assert(min_core_mapping < core_mapping_limit);
    for (auto i = min_core_mapping; i != core_mapping_limit; ++i) {
      core_to_num_pgs.emplace(i, 0);
    }
  }

private:
  std::map<core_id_t, unsigned> core_to_num_pgs;
  std::map<spg_t, core_id_t> pg_to_core;
};

/**
 * PGMap
 *
 * Maps spg_t to PG instance within a shard.  Handles dealing with waiting
 * on pg creation.
 */
class PGMap {
  struct PGCreationState : BlockerT<PGCreationState> {
    static constexpr const char * type_name = "PGCreation";

    void dump_detail(Formatter *f) const final;

    spg_t pgid;
    seastar::shared_promise<Ref<PG>> promise;
    bool creating = false;
    PGCreationState(spg_t pgid);

    PGCreationState(const PGCreationState &) = delete;
    PGCreationState(PGCreationState &&) = delete;
    PGCreationState &operator=(const PGCreationState &) = delete;
    PGCreationState &operator=(PGCreationState &&) = delete;

    ~PGCreationState();
  };

  std::map<spg_t, PGCreationState> pgs_creating;
  using pgs_t = std::map<spg_t, Ref<PG>>;
  pgs_t pgs;

public:
  using PGCreationBlocker = PGCreationState;
  using PGCreationBlockingEvent = PGCreationBlocker::BlockingEvent;
  /**
   * Get future for pg with a bool indicating whether it's already being
   * created.
   */
  std::pair<seastar::future<Ref<PG>>, bool>
  wait_for_pg(PGCreationBlockingEvent::TriggerI&&, spg_t pgid);

  /**
   * get PG in non-blocking manner
   */
  Ref<PG> get_pg(spg_t pgid);

  /**
   * Set creating
   */
  void set_creating(spg_t pgid);

  /**
   * Set newly created pg
   */
  void pg_created(spg_t pgid, Ref<PG> pg);

  /**
   * Add newly loaded pg
   */
  void pg_loaded(spg_t pgid, Ref<PG> pg);

  pgs_t& get_pgs() { return pgs; }
  const pgs_t& get_pgs() const { return pgs; }
  PGMap() = default;
  ~PGMap();
};

}
