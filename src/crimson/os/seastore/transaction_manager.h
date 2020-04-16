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


class LogicalCachedExtent : public CachedExtent {
protected:
  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LogicalCachedExtent(*this));
  };

  virtual void on_written(paddr_t record_block_offset) {
  }

  virtual extent_types_t get_type() {
    ceph_assert(0 == "TODO");
    return extent_types_t::NONE;
  }

  /**
   * Must return a valid delta usable in apply_delta() in submit_transaction
   * if state == PENDING_DELTA.
   */
  virtual ceph::bufferlist get_delta() {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  /**
   * bl is a delta obtained previously from get_delta.  The versions will
   * match.  Implementation should mutate buffer based on bl.
   */
  virtual void apply_delta(ceph::bufferlist &bl) {
    ceph_assert(0 == "TODO");
  }

  /**
   * Called on dirty CachedExtent implementation after replay.
   * Implementation should perform any reads/in-memory-setup
   * necessary. (for instance, the lba implementation uses this
   * to load in lba_manager blocks)
   */
  virtual complete_load_ertr::future<> complete_load() {
    ceph_assert(0 == "TODO");
    return complete_load_ertr::now();
  }

  
  
public:
  LogicalCachedExtent(
    ceph::bufferptr &&ptr) : CachedExtent(std::move(ptr)) {}

  void set_pin(LBAPinRef &&pin) {/* TODO */}
  LBAPin &get_pin() { return *((LBAPin*)nullptr); /* TODO */}

  laddr_t get_laddr() const { return laddr_t{0}; }

  void copy_in(ceph::bufferlist &bl, laddr_t off, loff_t len) {
    ceph_assert(off >= get_laddr());
    ceph_assert((off + len) <= (get_laddr() + get_bptr().length()));
    bl.begin().copy(len, get_bptr().c_str() + (off - get_laddr()));
  }

  void copy_in(LogicalCachedExtent &extent) {
    ceph_assert(extent.get_laddr() >= get_laddr());
    ceph_assert((extent.get_laddr() + extent.get_bptr().length()) <=
		(get_laddr() + get_bptr().length()));
    memcpy(
      get_bptr().c_str() + (extent.get_laddr() - get_laddr()),
      extent.get_bptr().c_str(),
      extent.get_length());
  }
};
using LogicalCachedExtentRef = TCachedExtentRef<LogicalCachedExtent>;
struct ref_laddr_cmp {
  using is_transparent = laddr_t;
  bool operator()(const LogicalCachedExtentRef &lhs,
		  const LogicalCachedExtentRef &rhs) const {
    return lhs->get_laddr() < rhs->get_laddr();
  }
  bool operator()(const laddr_t &lhs,
		  const LogicalCachedExtentRef &rhs) const {
    return lhs < rhs->get_laddr();
  }
  bool operator()(const LogicalCachedExtentRef &lhs,
		  const laddr_t &rhs) const {
    return lhs->get_laddr() < rhs;
  }
};
using lextent_set_t = addr_extent_set_base_t<
  laddr_t,
  LogicalCachedExtentRef,
  ref_laddr_cmp
  >;

template <typename T>
using lextent_list_t = addr_extent_list_base_t<
  laddr_t, TCachedExtentRef<T>>;

class TransactionManager : public JournalSegmentProvider {
  friend class Transaction;

  SegmentManager &segment_manager;
  Cache &cache;
  LBAManager &lba_manager;
  Journal &journal;

  using get_mutable_extent_ertr = SegmentManager::read_ertr;
  get_mutable_extent_ertr::future<LogicalCachedExtentRef> get_mutable_extent(
    Transaction &t,
    laddr_t offset,
    loff_t len);
    
public:
  TransactionManager(
    SegmentManager &segment_manager,
    Journal &journal,
    Cache &cache,
    LBAManager &lba_manager);

  segment_id_t next = 0;
  get_segment_ret get_segment() final {
    // TODO -- part of gc
    return get_segment_ret(
      get_segment_ertr::ready_future_marker{},
      next++);
  }

  void put_segment(segment_id_t segment) final {
    // TODO -- part of gc
    return;
  }

  using initialize_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  initialize_ertr::future<> initialize();

  using mount_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  mount_ertr::future<> mount();

  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  close_ertr::future<> close();

  TransactionRef create_transaction() {
    return lba_manager.create_transaction();
  }

  enum class mutate_result_t {
    SUCCESS,
    REFERENCED
  };

  /**
   * Read extents corresponding to specified lba range
   */
  using read_extent_ertr = SegmentManager::read_ertr;

  template <typename T>
  using read_extent_ret = read_extent_ertr::future<lextent_list_t<T>>;

  template <typename T>
  read_extent_ret<T> read_extents(
    Transaction &t,
    laddr_t offset,
    loff_t length)
  {
    std::unique_ptr<lextent_list_t<T>> ret;
    auto &ret_ref = *ret;
    std::unique_ptr<lba_pin_list_t> pin_list;
    auto &pin_list_ref = *pin_list;
    return lba_manager.get_mapping(
      t, offset, length
    ).safe_then([this, &t, &pin_list_ref, &ret_ref](auto pins) {
      pins.swap(pin_list_ref);
      return crimson::do_for_each(
	pin_list_ref.begin(),
	pin_list_ref.end(),
	[this, &t, &ret_ref](auto &pin) {
	  // TODO: invert buffer control so that we pass the buffer
	  // here into segment_manager to avoid a copy
	  return cache.get_extent<T>(
	    t,
	    pin->get_paddr(),
	    pin->get_length()
	  ).safe_then([this, &pin, &ret_ref](auto ref) mutable {
	    ref->set_pin(std::move(pin));
	    ret_ref.push_back(std::make_pair(ref->get_laddr(), ref));
	    return read_extent_ertr::now();
	  });
	});
    }).safe_then([this, ret=std::move(ret), pin_list=std::move(pin_list),
		  &t]() mutable {
      return read_extent_ret<T>(
	read_extent_ertr::ready_future_marker{},
	std::move(*ret));
    });
  }

  /**
   * Obtain mutable copy of extent
   *
   * TODO: add interface for exposing whether a delta needs to
   * be generated
   */
  CachedExtentRef get_mutable_extent(Transaction &t, CachedExtentRef ref) {
    return cache.duplicate_for_write(
      t,
      ref);
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
    LogicalCachedExtentRef &ref);
  inc_ref_ertr::future<> inc_ref(
    Transaction &t,
    laddr_t offset,
    loff_t len);

  /**
   * Remove refcount for range
   */
  using dec_ref_ertr = SegmentManager::read_ertr;
  dec_ref_ertr::future<> dec_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);
  dec_ref_ertr::future<> dec_ref(
    Transaction &t,
    laddr_t offset,
    loff_t len);

  /**
   * Allocate a new block of type T with an lba hint
   */
  using alloc_extent_ertr = SegmentManager::read_ertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_ertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t hint,
    loff_t len,
    bufferlist bl) {
    auto ext = cache.alloc_new_extent<T>(
      t,
      len);
    return lba_manager.alloc_extent_relative(
      t,
      hint,
      len,
      ext.get_paddr()
    ).safe_then([this, ext=std::move(ext)](auto &&ref) mutable {
      ext->set_pin(std::move(ref));
      return alloc_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  /**
   * Atomically submits transaction to persistence
   *
   * TODO: add ertr for retry due to transaction race
   *
   * @param 
   */
  using submit_transaction_ertr = crimson::errorator<
    crimson::ct_error::eagain, // Caller should retry transaction from beginning
    crimson::ct_error::input_output_error // Media error
    >;
  submit_transaction_ertr::future<> submit_transaction(TransactionRef);

  ~TransactionManager();
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
