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
  extent_version_t version; // changes to EXTENT_VERSION_NULL once invalidated
  laddr_t offset;
  ceph::bufferptr ptr;
public:

  void set_pin(LBAPinRef &&pin) {}
  LBAPin &get_pin() { return *pin_ref; }

  loff_t get_length() { return ptr.length(); }
  laddr_t get_addr() { return offset; }

  void copy_in(ceph::bufferlist &bl, laddr_t off, loff_t len) {
    ceph_assert(off > offset);
    ceph_assert((off + len) > get_length());
    ceph_assert(get_length() <= len);
    return bl.copy(0, len, ptr.c_str() + (off - offset));
  }
};
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

class ExtentSet {
  using extent_ref_list = std::list<CachedExtentRef>;
  extent_ref_list extents;
public:
  using iterator = extent_ref_list::iterator;
  using const_iterator = extent_ref_list::const_iterator;
  
  

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

class Cache {
public:
  std::pair<ExtentSet, ExtentSet> get_reserve_extents(
    laddr_t offset,
    loff_t length) {
    return std::make_pair(
      ExtentSet(),
      ExtentSet()
    );
  }
    
  std::ostream &print(
    std::ostream &out) const {
    return out;
  }
};

}
