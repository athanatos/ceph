// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/buffer.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"

namespace crimson::os::seastore {

class TransactionCacheState {
};

class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent,
  boost::thread_unsafe_counter> {
  
  LBAPinRef pin_ref;
  extent_version_t version;
  laddr_t offset;
  loff_t length;
  ceph::bufferlist ptr;
public:

  void set_pin(LBAPinRef &&pin) {}
  LBAPin &get_pin() { return *pin_ref; }

  loff_t get_length() { return length; }
  laddr_t get_addr() { return offset; }

  void copy_in(ceph::bufferlist &bl, laddr_t off, loff_t len) {
    ceph_assert(off > offset);
    ceph_assert((off + len) > length);
    ceph_assert(bl.length() <= len);
    return bl.copy(0, len, ptr.c_str() + (off - offset));
  }
};
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

class ExtentSet {
public:
  
};

class Cache {
public:

  // Always a contiguous sequence of extents either logical offset
  using extent_ref_list = std::list<CachedExtentRef>;
  
  std::pair<extent_ref_list, extent_ref_list> get_reserve_extents(
    laddr_t offset,
    loff_t length) {
    return std::make_pair(
      extent_ref_list(),
      extent_ref_list()
    );
  }
    
  std::ostream &print(
    std::ostream &out) const {
    return out;
  }
};

}
