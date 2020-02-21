// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/common/errorator.h"

namespace crimson::os::seastore {

class LBAPin {
public:
  virtual void set_paddr(paddr_t) = 0;
  
  virtual loff_t get_length() const = 0;
  virtual paddr_t get_paddr() const = 0;
  virtual laddr_t get_laddr() const = 0;

  virtual ~LBAPin() {}
};
using LBAPinRef = std::unique_ptr<LBAPin>;

using laddr_list_t = std::list<std::pair<laddr_t, loff_t>>;
using paddr_list_t = std::list<std::pair<paddr_t, segment_off_t>>;

using lba_pin_list_t = std::list<LBAPinRef>;
using lba_pin_ref_list_t = std::list<LBAPinRef&>;

class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent, boost::thread_unsafe_counter> {
  boost::intrusive::set_member_hook<> extent_index_hook;
  using index_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::set_member_hook<>,
    &CachedExtent::extent_index_hook>;
  using index = boost::intrusive::set<CachedExtent, index_member_options>;
  friend class ExtentIndex;

  boost::intrusive::list_member_hook<> primary_ref_list_hook;
  using primary_ref_list_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::list_member_hook<>,
    &CachedExtent::primary_ref_list_hook>;
  using list = boost::intrusive::list<
    CachedExtent,
    primary_ref_list_member_options>;
  friend class ExtentLRU;

  paddr_t poffset;
  ceph::bufferptr ptr;

  enum extent_state_t {
    PENDING,  // In Transaction::pending_index
    CLEAN,    // In Cache::extent_index
    INVALID   // Part of no ExtentIndex sets
  } state = extent_state_t::PENDING;

public:
  bool is_pending() const { return state == extent_state_t::PENDING; }

  void set_pin(LBAPinRef &&pin) {/* TODO: move into LBA specific subclass */}
  LBAPin &get_pin() { return *((LBAPin*)nullptr); /* TODO: move into LBA specific subclass */}

  loff_t get_length() { return ptr.length(); }
  laddr_t get_addr() { return laddr_t{0}; /* TODO: move into LBA specific subclass */}
  paddr_t get_paddr() { return poffset; }

  void copy_in(ceph::bufferlist &bl, laddr_t off, loff_t len) {
    #if 0
    // TODO: move into LBA specific subclass
    ceph_assert(off >= offset);
    ceph_assert((off + len) <= (offset + ptr.length()));
    bl.begin().copy(len, ptr.c_str() + (off - offset));
    #endif
  }

  void copy_in(CachedExtent &extent) {
    #if 0
    // TODO: move into LBA specific subclass
    ceph_assert(extent.offset >= offset);
    ceph_assert((extent.offset + extent.ptr.length()) <=
		(offset + ptr.length()));
    memcpy(
      ptr.c_str() + (extent.offset - offset),
      extent.ptr.c_str(),
      extent.get_length());
    #endif
  }

  friend bool operator< (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset < b.poffset;
  }

  friend bool operator> (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset > b.poffset;
  }

  friend bool operator== (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset == b.poffset;
  }
};
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

template <typename T>
class addr_extent_list_base_t
  : public std::list<std::pair<T, CachedExtentRef>> {
public:
  void merge(addr_extent_list_base_t &&other) {}
};

using lextent_list_t = addr_extent_list_base_t<laddr_t>;
using pextent_list_t = addr_extent_list_base_t<paddr_t>;

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

/**
 * Index of CachedExtent & by poffset, does not hold a reference,
 * user must ensure each extent is removed prior to deletion
 */
class ExtentIndex {
  CachedExtent::index extent_index;
public:
  void insert(CachedExtent &extent) {
    extent_index.insert(extent);
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

  std::pair<pextent_list_t, paddr_list_t> get_extents(paddr_list_t &l) {
    return std::make_pair(
      pextent_list_t(),
      paddr_list_t());
  }

  void add_to_read_set(const pextent_list_t &eset) {
    // TODO
  }
  void add_to_write_set(const pextent_list_t &eset) {
    // TODO
  }
};
using TransactionRef = std::unique_ptr<Transaction>;

class Cache {
  SegmentManager &segment_manager;
public:
  Cache(SegmentManager &segment_manager) : segment_manager(segment_manager) {}
  
  /**
   * get_extent
   */
  using get_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  template <typename T>
  using get_extent_ret = get_extent_ertr::future<TCachedExtentRef<T>>;

  template <typename T, typename F>
  get_extent_ret<T> get_extent(
    F &&f,                ///< [in] constructor
    Transaction &t,       ///< [in,out] current transaction
    paddr_t offset,       ///< [in] starting addr
    segment_off_t length  ///< [in] length
  ) {
    auto ptr = std::make_unique<bufferptr>(length);
    return segment_manager.read(
      offset,
      length,
      *ptr).safe_then(
	[this, ptr=std::move(ptr), f=std::forward<F>(f)]() mutable {
	  return std::move(f)(std::move(ptr));
	},
	crimson::ct_error::discard_all{});
  }

  template <typename T>
  TCachedExtentRef<T> alloc_new_extent(
    Transaction &t,
    segment_off_t length) {
    return *(static_cast<T*>(nullptr));
  }
  
private:
  /**
   * get_reserve_extents
   *
   * @param extents requested set of extents
   * @return <present, pending, fetch> caller is expected to perform reads
   *         for the extents in fetch, call present_reserved_extents on
   *         the result, and then call await_pending on pending
   */
  std::tuple<pextent_list_t, paddr_list_t, paddr_list_t> get_reserve_extents(
    paddr_list_t &extents);

  void present_reserved_extents(
    paddr_list_t &extents);

  using await_pending_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  // TODO: eio isn't actually important here, but we probably
  // want a way to signal that the original transaction isn't
  // going to complete the read
  using await_pending_fut = await_pending_ertr::future<pextent_list_t>;
  await_pending_fut await_pending(const paddr_list_t &pending);

public:
  using read_extent_ertr = SegmentManager::read_ertr;
  using read_extent_ret = read_extent_ertr::future<pextent_list_t>;
  read_extent_ret read_extents(
    Transaction &t,
    pextent_list_t &extents);
  

  /**
   * Allocates mutable buffer from extent_set on offset~len
   *
   * @param extent_set spanning extents obtained from get_reserve_extents
   * @param offset, len offset~len
   * @return mutable extent, may either be dirty or pending
   */
  template <typename T, typename F>
  CachedExtentRef duplicate_for_write(
    CachedExtentRef i) {
    return CachedExtentRef();
  }

  template <typename T, typename F>
  CachedExtentRef get_pending_extent_buffer(
    Transaction &t,
    segment_off_t length) {
    return CachedExtentRef();
  }

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
