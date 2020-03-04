// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/common/errorator.h"

namespace crimson::os::seastore {

/**
 * Cache
 *
 * This component is responsible for buffer management, including
 * transaction lifecycle.
 *
 * Seastore transactions are expressed as an atomic combination of
 * 1) newly written blocks
 * 2) logical mutations to existing physical blocks
 *
 * See record_t
 *
 * As such, any transaction has 3 components:
 * 1) read_set: references to extents read during the transaction
 * 2) write_set: references to extents which are either
 *    a) new physical blocks or
 *    b) mutations to existing physical blocks
 * 3) retired_set: extent refs to be retired either due to 2b or 
 *    due to releasing the extent generally.

 * In the case of 2b, the CachedExtent will have been copied into
 * a fresh CachedExtentRef such that the source extent ref is present
 * in the read set and the newly allocated extent is present in the
 * write_set.
 *
 * A transaction has 3 phases:
 * 1) construction: user calls Cache::get_transaction() and populates
 *    the returned transaction by calling Cache methods
 * 2) submission: user calls Cache::try_start_transaction().  If
 *    succcessful, the user may construct a record and submit the 
 *    transaction to the journal.
 * 3) completion: once the transaction is durable, the user must call
 *    Cache::complete_transaction() with the block offset to complete
 *    the transaction.
 *
 * Internally, in phase 1, the fields in Transaction are filled in.
 * - reads may block if the referenced extent is being written
 * - once a read obtains a particular CachedExtentRef for a paddr_t,
 *   it'll always get the same one until overwritten
 * - once a paddr_t is overwritten or written, subsequent reads of
 *   that addr will get the new ref
 *
 * In phase 2, if all extents in the read set are valid (not expired),
 * we can commit (otherwise, we fail and the user must retry).
 * - Expire all extents in the retired_set (they must all be valid)
 * - Remove all extents in the retired_set from Cache::extents
 * - Mark all extents in the write_set wait_io(), add promises to
 *   transaction
 * - Merge Transaction::write_set into Cache::extents
 *
 * After phase 2, the user will submit the record to the journal.
 * Once complete, we perform phase 3:
 * - For each CachedExtent in block_list, call
 *   CachedExtent::complete_initial_write(paddr_t) with the block's
 *   final offset (inferred from the extent's position in the block_list
 *   and extent lengths).
 * - For each block in mutation_list, call
 *   CachedExtent::delta_written(paddr_t) with the address of the start
 *   of the record
 * - Complete all promises with the final record start paddr_t
 */

class CachedExtent;
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;
class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent, boost::thread_unsafe_counter> {
  boost::intrusive::set_member_hook<> extent_index_hook;
  using index_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::set_member_hook<>,
    &CachedExtent::extent_index_hook>;
  using index = boost::intrusive::set<CachedExtent, index_member_options>;
  friend class ExtentIndex;
  friend class Transaction;

  boost::intrusive::list_member_hook<> primary_ref_list_hook;
  using primary_ref_list_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::list_member_hook<>,
    &CachedExtent::primary_ref_list_hook>;
  using list = boost::intrusive::list<
    CachedExtent,
    primary_ref_list_member_options>;
  friend class ExtentLRU;

  ceph::bufferptr ptr;

  /* number of deltas since initial write */
  extent_version_t version = EXTENT_VERSION_NULL;

  enum class extent_state_t : uint8_t {
    PENDING_INITIAL,  // In Transaction::write_set, or nothing while writing
    PENDING_DELTA,    // In Transaction::write_set, or nothing while writing
    WRITTEN,          // In Cache::extent_index
    INVALID           // Part of no ExtentIndex sets
  } state = extent_state_t::PENDING_INITIAL;

  /* address of original block -- relative in state PENDING_INITIAL */
  paddr_t poffset; 

  std::optional<seastar::shared_promise<>> io_wait_promise;

  void set_io_wait() {
    ceph_assert(!io_wait_promise);
    io_wait_promise = seastar::shared_promise<>();
  }

  void complete_io() {
    ceph_assert(io_wait_promise);
    io_wait_promise->set_value();
    io_wait_promise = std::nullopt;
  }

  seastar::future<> wait_io() {
    if (!io_wait_promise) {
      return seastar::now();
    } else {
      return io_wait_promise->get_shared_future();
    }
  }

protected:
  CachedExtent(ceph::bufferptr &&ptr) : ptr(std::move(ptr)) {}
  CachedExtent(const CachedExtent &other)
    : ptr(other.ptr.c_str(), other.ptr.length()),
      version(other.version),
      state(other.state),
      poffset(other.poffset) {}

  friend class Cache;
  template <typename T, typename... Args>
  static CachedExtentRef make_cached_extent_ref(Args&&... args) {
    return new T(std::forward<Args>(args)...);
  }

  void set_paddr(paddr_t offset) { poffset = offset; }

public:
  virtual CachedExtentRef duplicate_for_write() {
    return new CachedExtent(*this);
  }

  virtual void on_written(paddr_t record_block_offset) {}
  
  bool is_pending() const {
    return state == extent_state_t::PENDING_INITIAL ||
      state == extent_state_t::PENDING_DELTA;
  }

  paddr_t get_paddr() { return poffset; }
  size_t get_length() { return ptr.length(); }

  bufferptr &get_bptr() { return ptr; }

  friend bool operator< (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset < b.poffset;
  }

  friend bool operator> (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset > b.poffset;
  }

  friend bool operator== (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset == b.poffset;
  }
  friend struct paddr_cmp;
  friend struct ref_paddr_cmp;
};

struct paddr_cmp {
  bool operator()(paddr_t lhs, const CachedExtent &rhs) const {
    return lhs < rhs.poffset;
  }
  bool operator()(const CachedExtent &lhs, paddr_t rhs) const {
    return lhs.poffset < rhs;
  }
};

struct ref_paddr_cmp {
  using is_transparent = paddr_t;
  bool operator()(const CachedExtentRef &lhs, const CachedExtentRef &rhs) const {
    return lhs->poffset < rhs->poffset;
  }
  bool operator()(const paddr_t &lhs, const CachedExtentRef &rhs) const {
    return lhs < rhs->poffset;
  }
  bool operator()(const CachedExtentRef &lhs, const paddr_t &rhs) const {
    return lhs->poffset < rhs;
  }
};

template <typename T, typename C>
class addr_extent_list_base_t
  : public std::list<std::pair<T, C>> {
public:
  void merge(addr_extent_list_base_t &&other) { /* TODO */ }
};

using pextent_list_t = addr_extent_list_base_t<paddr_t, CachedExtentRef>;

template <typename T, typename C, typename Cmp>
class addr_extent_set_base_t
  : public std::set<C, Cmp> {
public:
  void merge(addr_extent_set_base_t &&other) { /* TODO */ }
};

using pextent_set_t = addr_extent_set_base_t<
  paddr_t,
  CachedExtentRef,
  ref_paddr_cmp
  >;

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

template <typename T>
using t_pextent_list_t = addr_extent_list_base_t<paddr_t, TCachedExtentRef<T>>;

/**
 * Index of CachedExtent & by poffset, does not hold a reference,
 * user must ensure each extent is removed prior to deletion
 */
class ExtentIndex {
  friend class Cache;
  CachedExtent::index extent_index;
public:
  auto get_overlap(paddr_t addr, segment_off_t len) {
    return std::make_pair(extent_index.end(), extent_index.end());
  }

  void clear() {
    extent_index.clear();
  }

  void insert(CachedExtent &extent) {
    // sanity check
    auto [a, b] = get_overlap(
      extent.get_paddr(),
      extent.get_length());
    ceph_assert(a == b);

    extent_index.insert(extent);
  }

  void erase(CachedExtent &extent) {
    extent_index.erase(extent);
  }

  auto find_offset(paddr_t offset) {
    return extent_index.find(offset, paddr_cmp());
  }

  auto end() {
    return extent_index.end();
  }

  void merge(ExtentIndex &&other) {
    for (auto it = other.extent_index.begin();
	 it != other.extent_index.end();
	 ) {
      auto &ext = *it;
      ++it;
      other.extent_index.erase(ext);
      extent_index.insert(ext);
    }
  }

  template <typename T>
  void remove(T &l) {
    for (auto &ext : l) {
      extent_index.erase(l);
    }
  }
};

class Transaction {
  friend class Cache;

  segment_off_t offset = 0;

  pextent_set_t read_set;
  ExtentIndex write_set;

  std::list<CachedExtentRef> fresh_block_list;
  std::list<CachedExtentRef> mutated_block_list;

  pextent_set_t retired_set;

  CachedExtentRef get_extent(paddr_t addr) {
    ceph_assert(retired_set.count(addr) == 0);
    if (auto iter = write_set.find_offset(addr);
	iter != write_set.end()) {
      return CachedExtentRef(&*iter);
    } else if (
      auto iter = read_set.find(addr);
      iter != read_set.end()) {
      return *iter;
    } else {
      return CachedExtentRef();
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    ceph_assert(retired_set.count(ref->get_paddr()) == 0);
    retired_set.insert(ref);
  }

  void add_to_read_set(CachedExtentRef ref) {
    ceph_assert(read_set.count(ref) == 0);
    read_set.insert(ref);
  }

  void add_fresh_extent(CachedExtentRef ref) {
    fresh_block_list.push_back(ref);
    ref->set_paddr(make_relative_paddr(offset));
    offset += ref->get_length();
  }
};
using TransactionRef = std::unique_ptr<Transaction>;

class Cache {
  SegmentManager &segment_manager;
  ExtentIndex extents;

  bufferptr alloc_cache_buf(size_t size) {
    return ceph::bufferptr(size);
  }
public:
  Cache(SegmentManager &segment_manager) : segment_manager(segment_manager) {}

  TransactionRef get_transaction() {
    return std::make_unique<Transaction>();
  }
  
  /**
   * get_extent
   */
  using get_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  template <typename T>
  get_extent_ertr::future<TCachedExtentRef<T>> get_extent(
    Transaction &t,       ///< [in,out] current transaction
    paddr_t offset,       ///< [in] starting addr
    segment_off_t length  ///< [in] length
  ) {
    if (auto i = t.get_extent(offset)) {
      return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	TCachedExtentRef<T>(static_cast<T*>(&*i)));
    } else if (auto iter = extents.find_offset(offset);
	       iter != extents.end()) {
      auto ret = TCachedExtentRef<T>(static_cast<T*>(&*iter));
      return ret->wait_io().then([&t, ret=std::move(ret)]() mutable {
	t.add_to_read_set(ret);
	return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	  std::move(ret));
      });
    } else {
      auto ref = CachedExtent::make_cached_extent_ref<T>(
	alloc_cache_buf(length));
      ref->set_io_wait();
      return segment_manager.read(
	offset,
	length,
	ref->get_bptr()).safe_then(
	  [this, &t, ref=std::move(ref)]() mutable {
	    ref->complete_io();
	    t.add_to_read_set(ref);
	    return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	      TCachedExtentRef<T>(
		static_cast<T*>(ref.detach())));
	    /* TODO: check I didn't break the refcounting */
	  },
	  get_extent_ertr::pass_further{},
	  crimson::ct_error::discard_all{});
    }
  }

  template<typename T>
  get_extent_ertr::future<t_pextent_list_t<T>> get_extents(
    Transaction &t,
    paddr_list_t &&extents) {
    auto retref = std::make_unique<t_pextent_list_t<T>>();
    auto &ret = *retref;
    auto ext = std::make_unique<paddr_list_t>(std::move(extents));
    return crimson::do_for_each(
      ext->begin(),
      ext->end(),
      [this, &t, &ret](auto &p) {
	auto &[offset, len] = p;
	return get_extent(t, offset, len).safe_then([&ret](auto cext) {
	  ret.push_back(std::move(cext));
	});
      }).safe_then([retref=std::move(retref), ext=std::move(ext)]() mutable {
	return get_extent_ertr::make_ready_future<t_pextent_list_t<T>>(
	  std::move(*retref));
      });
  }
  
  template <typename T>
  TCachedExtentRef<T> alloc_new_extent(
    Transaction &t,
    segment_off_t length) {
    auto ret = CachedExtent::make_cached_extent_ref<T>(
      alloc_cache_buf(length));
    t.add_fresh_extent(ret);
    return ret;
  }

  /**
   * Allocates mutable buffer from extent_set on offset~len
   *
   * @param current transaction
   * @param extent to duplicate
   * @return mutable extent
   */
  CachedExtentRef duplicate_for_write(
    Transaction &t,
    CachedExtentRef i);

  bool try_begin_commit(Transaction &t);
  void complete_commit(
    Transaction &t,
    paddr_t final_block_start);

  using replay_delta_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using replay_delta_ret = replay_delta_ertr::future<>;
  replay_delta_ret replay_delta(const delta_info_t &delta);
    
  std::ostream &print(
    std::ostream &out) const {
    return out;
  }
};

}
