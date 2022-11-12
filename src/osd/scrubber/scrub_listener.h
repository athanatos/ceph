// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <set>

#include "osd/scrubber/ScrubStore.h"
#include "osd/osd_types.h"

namespace Scrub {

/**
 * ScrubListener
 *
 * Interface by which PgScrubber propogates external events and
 * accesses external interfaces.
 */
class ScrubListener {
public:
  /// get spg_t for pg
  virtual spg_t sl_get_spgid() const = 0;

  /// returns true iff pg has reset peering since e.
  virtual bool sl_has_reset_since(epoch_t e) const = 0;

  /// return reference to current osdmap
  virtual const OSDMapRef &sl_get_osdmap() const = 0;

  /// get current osdmap epoch
  epoch_t sl_get_osdmap_epoch() const { return sl_get_osdmap()->get_epoch(); }

  /// get reference to PGPool
  virtual const PGPool &sl_get_pool() const = 0;

  /// get config reference
  virtual const ConfigProxy &sl_get_config() const = 0;

  /// get reference to current pg info
  virtual const pg_info_t &sl_get_info() const = 0;

  /// return false if other threads may access concurrently
  virtual bool sl_is_locked() const = 0;

  /// returns whether this osd is currently the primary for the pg
  virtual bool sl_is_primary() const = 0;

  /// forces osd to notice pg stat updates (including pg state)
  virtual void sl_publish_stats_to_osd() = 0;

  /// returns greatest log entry version applied locally
  virtual eversion_t sl_get_last_update_applied() const = 0;

  /// returns true if there are active ops blocked by scrub
  virtual bool sl_ops_blocked_by_scrub() const = 0;

  /// returns target priority based on whether ops are blocked
  virtual Scrub::scrub_prio_t sl_get_block_priority() const = 0;

  /**
   * sl_objects_list_partial
   *
   * Lists n objects in pg starting at begin (inclusive) where
   * min <= n <= max.
   */
  virtual int sl_objects_list_partial(
    const hobject_t& begin,     ///< [in] start object
    int min,                    ///< [in] min to list
    int max,                    ///< [in] max to list
    std::vector<hobject_t>* ls, ///< [out] results
    hobject_t* next             ///< [out] next after results
  ) = 0; ///< @return error code

  /// list objects in [start, end)
  virtual int sl_objects_list_range(
    const hobject_t& start,            /// [in] start bound
    const hobject_t& end,              /// [in] end bound (exclusive)
    std::vector<hobject_t>* ls,        /// [out] results
    std::vector<ghobject_t>* gen_obs=0 /// [out] non-max generation results
  ) = 0; /// @return error code

  /// scan next object pointed to by pos, increment pos
  virtual int sl_scan_list(
    ScrubMap& map,       ///< [out] results
    ScrubMapBuilder& pos ///< [in] objects to scan + iterator into that list
  ) = 0; /// @return error code

  /**
   * sl_range_available_for_scrub
   *
   * Returns whether the passed range is ready to be scrubbed.  If
   * unavailable, it will return false *and* arrange for scrub to
   * be rescheduled when that changes.
   *
   * @return whether objects in [begin, end) are available for scrub
   */
  virtual bool sl_range_available_for_scrub(
    const hobject_t& begin, ///< [in] range start, inclusive
    const hobject_t& end    ///< [in] range end, exclusive
  ) = 0;

  /**
   * sl_get_latest_update_in_range
   *
   * Returns the version of the most recent update to an object
   * in [begin, end).
   *
   * @return version of most recent update affecting an object in
   *         [start, end).  eversion_t{} if no such event is found.
   *         If there is such an event >= get_last_update_applied(),
   *         it is guarranteed to be found.
   */
  virtual eversion_t sl_get_latest_update_in_range(
    const hobject_t& start, ///< [in] start of range, inclusive
    const hobject_t& end    ///< [in] end of range, exclusive
  ) = 0;

  /// get current acting set for pg
  virtual std::set<pg_shard_t> sl_get_actingset() const = 0;

  /// returns last_peering_reset
  virtual epoch_t sl_get_last_peering_reset() const = 0;

  /// reset stat fields related to scrub progress
  virtual void sl_reset_in_progress_scrub_stats() = 0;

  /// add <count> objects scrubbed to in progress stats
  virtual void sl_report_objects_scrubbed(int64_t count) = 0;

  /// Update history and stats
  virtual void sl_update_stats(
    std::function<bool(pg_history_t &, pg_stat_t &)> &&f,
    ObjectStore::Transaction *t = nullptr) = 0;

  /// Get ScrubStore instance
  virtual Scrub::Store::Ref sl_get_scrub_store(
    ObjectStore::Transaction &t
  ) = 0;

  virtual ~ScrubListener() = default;
};

}
