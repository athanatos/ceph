// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

class CachedExtent;
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

/**
 * CachedExtent
 */
class ExtentIndex;
class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent, boost::thread_unsafe_counter> {
  friend class ExtentIndex;
  ExtentIndex *parent_index = nullptr;

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
    INITIAL_WRITE_PENDING, // In Transaction::write_set and fresh_block_list
    MUTATION_PENDING,      // In Transaction::write_set and mutated_block_list
    CLEAN,                 // In Cache::extent_index, Transaction::read_set
                           //  during write, contents match disk, version == 0
    DIRTY,                 // Same as CLEAN, but contents do not match disk,
                           //  version > 0
    INVALID                // Part of no ExtentIndex set
  } state = extent_state_t::INVALID;
  friend std::ostream &operator<<(std::ostream &, extent_state_t);

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
  static TCachedExtentRef<T> make_cached_extent_ref(Args&&... args) {
    return new T(std::forward<Args>(args)...);
  }

  void set_paddr(paddr_t offset) { poffset = offset; }

  extent_version_t get_version() const {
    return version;
  }

public:
  virtual CachedExtentRef duplicate_for_write() = 0;

  virtual void prepare_write() {}

  virtual void on_written(paddr_t record_block_offset) = 0;

  virtual extent_types_t get_type() const = 0;

  friend std::ostream &operator<<(std::ostream &, extent_state_t);
  std::ostream &print(std::ostream &out) const {
    return out << "CachedExtent(addr=" << this
	       << ", type=" << get_type()
	       << ", version=" << version
	       << ", paddr=" << get_paddr()
	       << ", state=" << state
	       << ")";
  }

  /**
   * Must return a valid delta usable in apply_delta() in submit_transaction
   * if state == PENDING_DELTA.
   */
  virtual ceph::bufferlist get_delta() = 0;

  /**
   * bl is a delta obtained previously from get_delta.  The versions will
   * match.  Implementation should mutate buffer based on bl.
   */
  virtual void apply_delta(ceph::bufferlist &bl) = 0;

  /**
   * Called on dirty CachedExtent implementation after replay.
   * Implementation should perform any reads/in-memory-setup
   * necessary. (for instance, the lba implementation will use this
   * to load in lba_manager blocks)
   */
  using complete_load_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual complete_load_ertr::future<> complete_load() = 0;

  template <typename T>
  TCachedExtentRef<T> cast() {
    return TCachedExtentRef<T>(static_cast<T*>(this));
  }

  bool is_pending() const {
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
      state == extent_state_t::MUTATION_PENDING;
  }

  bool is_clean() const {
    ceph_assert(is_valid());
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
      state == extent_state_t::CLEAN;
  }

  bool is_dirty() const {
    ceph_assert(is_valid());
    return !is_clean();
  }

  bool is_valid() const {
    return state != extent_state_t::INVALID;
  }

  paddr_t get_paddr() const { return poffset; }
  loff_t get_length() const { return static_cast<loff_t>(ptr.length()); }

  bufferptr &get_bptr() { return ptr; }
  const bufferptr &get_bptr() const { return ptr; }

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

  virtual ~CachedExtent();
};

std::ostream &operator<<(std::ostream &, CachedExtent::extent_state_t);
std::ostream &operator<<(std::ostream &, const CachedExtent&);

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
  void merge(addr_extent_list_base_t &&other) {
    ceph_assert(0);
    /* TODO */
  }
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
    extent.parent_index = this;
  }

  void erase(CachedExtent &extent) {
    extent_index.erase(extent);
    extent.parent_index = nullptr;
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


}
