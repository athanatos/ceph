// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/common/errorator.h"

namespace crimson::os::seastore {

class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent,
  boost::thread_unsafe_counter> {
  
  LBAPinRef pin_ref;
  extent_version_t version; // changes to EXTENT_VERSION_NULL once invalidated
  laddr_t offset;
  paddr_t poffset;
  ceph::bufferptr ptr;

  enum extent_state_t {
    PENDING,
    CLEAN,
    DIRTY,
    INVALID
  } state = extent_state_t::PENDING;

  std::list<delta_info_t> pending_deltas;
public:
  bool is_pending() const { return state == extent_state_t::PENDING; }

  void set_pin(LBAPinRef &&pin) {}
  LBAPin &get_pin() { return *pin_ref; }

  loff_t get_length() { return ptr.length(); }
  laddr_t get_addr() { return offset; }
  paddr_t get_paddr() { return poffset; }

  void copy_in(ceph::bufferlist &bl, laddr_t off, loff_t len) {
    ceph_assert(off >= offset);
    ceph_assert((off + len) <= (offset + ptr.length()));
    bl.copy(0, len, ptr.c_str() + (off - offset));
  }

  void copy_in(CachedExtent &extent) {
    ceph_assert(extent.offset >= offset);
    ceph_assert((extent.offset + extent.ptr.length()) <=
		(offset + ptr.length()));
    memcpy(
      ptr.c_str() + (extent.offset - offset),
      extent.ptr.c_str(),
      extent.get_length());
  }

  void add_pending_delta(delta_info_t &&delta) {
    pending_deltas.push_back(std::move(delta));
  }
};
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

class ExtentSet {
  using extent_ref_list = std::list<CachedExtentRef>;
  extent_ref_list extents;
public:
  using iterator = extent_ref_list::iterator;
  using const_iterator = extent_ref_list::const_iterator;

  ExtentSet() = default;
  ExtentSet(CachedExtentRef &ref) : extents{{ref}} {}

  void merge(ExtentSet &&other) { /* TODO */ }

  void insert(CachedExtentRef &ref) { /* TODO */ }
  void insert(const ExtentSet &other) { /* TODO */ }

  iterator begin() {
    return extents.begin();
  }

  const_iterator begin() const {
    return extents.begin();
  }

  iterator end() {
    return extents.end();
  }

  const_iterator end() const {
    return extents.end();
  }
};

/* TODO: replace with something that mostly doesn't allocate,
   often used for single elements */
using extent_list_t = std::list<std::pair<laddr_t, loff_t>>;

class Cache {
public:
  /**
   * get_reserve_extents
   *
   * @param extents requested set of extents
   * @return <present, pending, fetch> caller is expected to perform reads
   *         for the extents in fetch, call present_reserved_extents on
   *         the result, and then call await_pending on pending
   */
  std::tuple<ExtentSet, extent_list_t, extent_list_t> get_reserve_extents(
    const extent_list_t &extents);

  void present_reserved_extents(
    ExtentSet &extents);

  using await_pending_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  // TODO: eio isn't actually important here, but we probably
  // want a way to signal that the original transaction isn't
  // going to complete the read
  using await_pending_fut = await_pending_ertr::future<ExtentSet>;
  await_pending_fut await_pending(const extent_list_t &pending);

  /**
   * Allocates mutable buffer from extent_set on offset~len
   *
   * @param extent_set spanning extents obtained from get_reserve_extents
   * @param offset, len offset~len
   * @return mutable extent, may either be dirty or pending
   */
  CachedExtentRef duplicate_for_write(
    ExtentSet &extent_set,
    laddr_t offset,
    loff_t len) {
    return CachedExtentRef();
  }

  void update_extents(
    ExtentSet &extents,
    const std::list<std::pair<paddr_t, segment_off_t>> &to_release);

  CachedExtentRef get_extent_buffer(
    laddr_t offset,
    loff_t length);

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
