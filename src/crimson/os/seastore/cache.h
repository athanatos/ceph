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

protected:
  CachedExtent(ceph::bufferptr &&ptr) : ptr(std::move(ptr)) {}

  friend class Cache;
  template <typename... Args>
  static CachedExtentRef make_cached_extent_ref(Args&&... args) {
    return new CachedExtent(std::forward<Args>(args)...);
  }

public:
  bool is_pending() const { return state == extent_state_t::PENDING; }

  void set_pin(LBAPinRef &&pin) {/* TODO: move into LBA specific subclass */}
  LBAPin &get_pin() { return *((LBAPin*)nullptr); /* TODO: move into LBA specific subclass */}

  loff_t get_length() { return ptr.length(); }
  laddr_t get_addr() { return laddr_t{0}; /* TODO: move into LBA specific subclass */}
  paddr_t get_paddr() { return poffset; }

  bufferptr &get_bptr() { return ptr; }

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
  friend struct paddr_cmp;
};

struct paddr_cmp {
  bool operator()(paddr_t lhs, const CachedExtent &rhs) const {
    return lhs < rhs.poffset;
  }
  bool operator()(const CachedExtent &lhs, paddr_t rhs) const {
    return lhs.poffset < rhs;
  }
};

template <typename T, typename C>
class addr_extent_list_base_t
  : public std::list<std::pair<T, C>> {
public:
  void merge(addr_extent_list_base_t &&other) { /* TODO */ }
};

using lextent_list_t = addr_extent_list_base_t<laddr_t, CachedExtentRef>;
using pextent_list_t = addr_extent_list_base_t<paddr_t, CachedExtentRef>;

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

template <typename T>
using t_pextent_list_t = addr_extent_list_base_t<paddr_t, TCachedExtentRef<T>>;

template <typename T>
using t_lextent_list_t = addr_extent_list_base_t<laddr_t, TCachedExtentRef<T>>;

/**
 * Index of CachedExtent & by poffset, does not hold a reference,
 * user must ensure each extent is removed prior to deletion
 */
class ExtentIndex {
  friend class Cache;
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

  CachedExtentRef get_extent(paddr_t addr) {
    return CachedExtentRef();
  }

  std::pair<pextent_list_t, paddr_list_t> get_extent(const paddr_list_t &l) {
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
  ExtentIndex extents;
public:
  Cache(SegmentManager &segment_manager) : segment_manager(segment_manager) {}
  
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

  template <typename T, typename F>
  pextent_list_t create_pending_exents(
    paddr_list_t &addrs) {
    return pextent_list_t();
  }

  using await_pending_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  // TODO: eio isn't actually important here, but we probably
  // want a way to signal that the original transaction isn't
  // going to complete the read
  using await_pending_fut = await_pending_ertr::future<pextent_list_t>;
  await_pending_fut await_pending(const paddr_list_t &pending);

  using read_extent_ertr = SegmentManager::read_ertr;
  using read_extent_ret = read_extent_ertr::future<pextent_list_t>;
  read_extent_ret read_extents(
    paddr_list_t &extents);

public:
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
      // TODO
      return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	TCachedExtentRef<T>(static_cast<T*>(&*i)));
    } else if (auto iter = extents.extent_index.find(offset, paddr_cmp());
	       iter != extents.extent_index.end()) {
      return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	TCachedExtentRef<T>(static_cast<T*>(&*iter)));
    } else {
      auto ref = T::make_cached_extent_ref(ceph::bufferptr(length));
      return segment_manager.read(
	offset,
	length,
	ref->get_bptr()).safe_then(
	  [this, ref=std::move(ref)] {
	    return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	      std::move(ref));
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
    return *(static_cast<T*>(nullptr));
  }

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
