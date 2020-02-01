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

class Transaction {
  friend class TransactionManager;
  
  ExtentSet read_set;
  record_t pending_record;
public:
  void add_to_read_set(const ExtentSet &eset) { /* TODO */ }
  std::pair<ExtentSet, extent_list_t>
  get_extents(const extent_list_t &eset) {
    return {ExtentSet{}, extent_list_t{}};
  }
};
using TransactionRef = std::unique_ptr<Transaction>;

class TransactionManager {
  Cache cache;
  SegmentManager &segment_manager;
  LBAManagerRef lba_manager;
  std::unique_ptr<Journal> journal;

  ExtentSet read_set;
  ExtentSet write_set;

  using read_extent_ertr = SegmentManager::read_ertr;
  using read_extent_ret = read_extent_ertr::future<ExtentSet>;
  read_extent_ret read_extents(
    Transaction &t,
    const extent_list_t &extents);

  using get_mutable_extent_ertr = SegmentManager::read_ertr;
  get_mutable_extent_ertr::future<CachedExtentRef> get_mutable_extents(
    Transaction &t,
    const extent_list_t &extents);
    
public:
  TransactionManager(SegmentManager &segment_manager);

  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> init();

  TransactionRef create_transaction() {
    return std::make_unique<Transaction>();
  }

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
  mutate_ertr::future<> mutate(
    Transaction &t,
    extent_types_t type,
    laddr_t offset,
    loff_t len,
    F &&f) {
    return get_mutable_extents(
      t, {{offset, len}}
    ).safe_then([this, &t, f=std::move(f)](auto &extent) mutable {
      auto bl = f(extent->ptr);
      // remember bl;
      return mutate_ertr::now();
    });
  }
  
  /**
   * Add operation replacing specified extent
   *
   * offset, len must be aligned, offset~len must be
   * allocated
   */
  using replace_ertr = SegmentManager::read_ertr;
  replace_ertr::future<> replace(
    Transaction &t,
    laddr_t offset,
    loff_t len,
    bufferlist bl) {
    // pull relevant portions of lba tree
    return replace_ertr::now();
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
