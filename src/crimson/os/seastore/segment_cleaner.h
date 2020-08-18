// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/cached_extent.h"



namespace crimson::os::seastore {
class Transaction;

class SegmentCleaner : public JournalSegmentProvider {
public:
  /// Config
  struct config_t {
    size_t num_segments = 0;
    size_t segment_size = 0;
    size_t target_journal_segments = 0;
    size_t max_journal_segments = 0;

    static config_t default_from_segment_manager(
      SegmentManager &manager) {
      return config_t{
	manager.get_num_segments(),
	static_cast<size_t>(manager.get_segment_size()),
	2,
	4};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    /**
      get_next_dirty_extent
     *
     * Returns the dirty extent with the oldest delta.  Empty
     * CahcedExtentRef if none.
     */
    virtual CachedExtentRef get_next_dirty_extent() = 0;

    /**
     * get_next_live_extents
     *
     * Returns some number (usually one record worth) of extents
     * which are known to still be live beginning at after limited
     * to the segment containing after.
     *
     * This operation is non-transactional.  Returned segments will
     * be a superset of live segments at time of invocation.
     * rewrite_extent will correctly handle invalid or non-live
     * segements when invoked.
     */
    using get_next_live_extents_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    using get_next_live_extents_ret = get_next_live_extents_ertr::future<
      pextent_list_t>;
    virtual get_next_live_extents_ret get_next_live_extents(
      paddr_t after) = 0;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  Segments in extents_to_relocate may be invalid,
     * implementation must correctly handle finding the current
     * instance of any that are still live and ignore any that are
     * no longer live.
     */
    using rewrite_extent_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    using rewrite_extent_ret = rewrite_extent_ertr::future<>;
    virtual rewrite_extent_ret rewrite_extent(
      Transaction &t,
      CachedExtentRef extent) = 0;
  };

private:
  segment_id_t next = 0;
  config_t config;

  journal_seq_t journal_tail_target;
  journal_seq_t journal_tail_committed;
  segment_seq_t max_journal_segment_seq = 0;

  std::unique_ptr<ExtentCallbackInterface> ecb;

public:
  SegmentCleaner(config_t config)
    : config(config) {}

  get_segment_ret get_segment() final;

  void put_segment(segment_id_t segment) final;

  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  void update_journal_tail_committed(journal_seq_t committed) final {
    if (committed > journal_tail_committed) {
      journal_tail_committed = committed;
    }
  }

  void update_journal_tail_target(journal_seq_t target) {
    assert(target >= journal_tail_target);
    if (target > journal_tail_target) {
      journal_tail_target = target;
    }
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb.reset(cb);
  }

  /**
   * do_immediate_work
   *
   * Should be invoked prior to submission of any transaction,
   * will piggy-back work required to maintain deferred work
   * constraints.
   */
  using do_immediate_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_immediate_work_ret = do_immediate_work_ertr::future<>;
  do_immediate_work_ret do_immediate_work(
    Transaction &t);

  
  /**
   * do_deferred_work
   *
   * Should be called at idle times -- will perform background
   * operations based on deferred work constraints.
   *
   * If returned timespan is non-zero, caller should pause calling
   * back into do_deferred_work before returned timespan has elapsed,
   * or a foreground operation occurs.
   */
  using do_deferred_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_deferred_work_ret = do_deferred_work_ertr::future<
    ceph::timespan
    >;
  do_deferred_work_ret do_deferred_work(
    Transaction &t);

private:
  size_t get_journal_score_target() const {
    return config.target_journal_segments * config.segment_size;
  }

  size_t get_journal_score_limit() const {
    return config.max_journal_segments * config.segment_size;
  }

  size_t get_current_journal_score() const {
    return (
      (max_journal_segment_seq - journal_tail_target.segment_seq - 1) *
      config.segment_size
    ) - static_cast<size_t>(journal_tail_target.offset.offset);
  } 
};

}
