// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <limits>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/common/type_helpers.h"
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

  struct PGPlacement : BlockerT<PGPlacement> {
    static constexpr const char * type_name = "PGPlacement";
    const spg_t pgid;

    /* While is PG is being created (or loaded), contains a promise that
     * will become available once the PG is ready.  Otherwise, nullopt. */
    std::optional<seastar::shared_promise<>> on_available;

    // null until created, only accessible on specified core
    Ref<PG> pg;

    PGPlacement(spg_t pgid);
    ~PGPlacement();

    void set_created(Ref<PG> _pg);

    PGPlacement(const PGPlacement &) = delete;
    PGPlacement(PGPlacement &&) = delete;
    PGPlacement &operator=(const PGPlacement &) = delete;
    PGPlacement &operator=(PGPlacement &&) = delete;

    void dump_detail(Formatter *f) const final;
  };

  std::map<spg_t, PGPlacement> pgs;

public:
  using PGCreationBlocker = PGPlacement;
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

  auto get_num_pgs() const { return pgs.size(); }

  PGMap() = default;
  ~PGMap();
};

}
