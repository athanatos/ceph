// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/journal.h"


namespace crimson::os::seastore {
class Journal;

class TransactionManager {
  friend class Transaction;

  SegmentManager &segment_manager;
  Cache &cache;
  LBAManagerRef lba_manager;
  std::unique_ptr<Journal> journal;

#if 0
  using read_extent_ertr = SegmentManager::read_ertr;
  using read_extent_ret = read_extent_ertr::future<ExtentSet>;
  read_extent_ret read_extents(
    Transaction &t,
    const extent_list_t &extents);
#endif

  using get_mutable_extent_ertr = SegmentManager::read_ertr;
  get_mutable_extent_ertr::future<CachedExtentRef> get_mutable_extent(
    Transaction &t,
    laddr_t offset,
    loff_t len);
    
public:
  TransactionManager(SegmentManager &segment_manager, Cache &cache);

  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> init();

  TransactionRef create_transaction() {
    return lba_manager->create_transaction();
  }

  enum class mutate_result_t {
    SUCCESS,
    REFERENCED
  };

  /**
   * Add operation mutating specified extent
   *
   * offset~len must be an extent previously returned from alloc_extent
   *
   * f(current) should mutate the bytes of current and return a
   * bufferlist suitable for a record delta
   */
  using mutate_ertr = SegmentManager::read_ertr;
  template <typename F>
  mutate_ertr::future<mutate_result_t> mutate(
    Transaction &t,
    extent_types_t type,
    laddr_t offset,
    loff_t len,
    F &&f) {
    return get_mutable_extent(
      t, offset, len
    ).safe_then([this, type, offset, &t, f=std::move(f)](auto extent) mutable {
      auto bl = f(extent->ptr);
      if (!extent->is_pending()) {
	#if 0
	// TODO: add to lba specific subclass
	extent->add_pending_delta(
	  {type, offset, extent->get_poffset(), std::move(bl)}
	);
	#endif
      }
      return replace_ertr::make_ready_future<mutate_result_t>(
	mutate_result_t::SUCCESS);
    });
  }
  
  /**
   * Add operation replacing specified extent
   *
   * offset, len must be aligned, offset~len must be
   * allocated
   */
  using replace_ertr = SegmentManager::read_ertr;
  replace_ertr::future<mutate_result_t> replace(
    Transaction &t,
    laddr_t offset,
    loff_t len,
    bufferlist bl) {
    // pull relevant portions of lba tree
    return replace_ertr::make_ready_future<mutate_result_t>(
      mutate_result_t::SUCCESS);
  }

  /**
   * Add refcount for range
   */
  using inc_ref_ertr = SegmentManager::read_ertr;
  inc_ref_ertr::future<> inc_ref(
    Transaction &t,
    laddr_t offset,
    loff_t len) {
    // pull relevant portion of lba tree
    return inc_ref_ertr::now();
  }

  /**
   * Remove refcount for range
   */
  using dec_ref_ertr = SegmentManager::read_ertr;
  dec_ref_ertr::future<> dec_ref(
    Transaction &t,
    laddr_t offset,
    loff_t len) {
    // pull relevant portion of lba tree
    return dec_ref_ertr::now();
  }

  using alloc_extent_ertr = SegmentManager::read_ertr;
  alloc_extent_ertr::future<laddr_t> alloc_extent(
    Transaction &t,
    laddr_t hint,
    loff_t len,
    bufferlist bl) {
    // pull relevant portion of lba tree
    return alloc_extent_ertr::make_ready_future<laddr_t>();
  }

  /**
   * Atomically submits transaction to persistence
   *
   * TODO: add ertr for retry due to transaction race
   *
   * @param 
   */
  using submit_transaction_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  submit_transaction_ertr::future<> submit_transaction(TransactionRef &&);

  ~TransactionManager();
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
