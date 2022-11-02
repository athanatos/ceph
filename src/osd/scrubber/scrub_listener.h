// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

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
   */
  virtual bool sl_range_available_for_scrub(
    const hobject_t &begin, ///< [in] range start, inclusive
    const hobject_t &end    ///< [in] range end, exclusive
  ) = 0;

  virtual ~ScrubListener() = default;
};

}
