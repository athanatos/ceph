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
  ceph::bufferptr ptr;
public:
};
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

class ExtentSet {
public:
  
};

class Cache {
public:

  // Always a contiguous sequence of extents either logical offset
  using extent_ref_list = std::list<CachedExtent>;
  
  extent_ref_list get_logical_extents_for_write(
    laddr_t offset,
    size_t length) {
    return std::list<CachedExtent>();
  }
    
  std::ostream &print(
    std::ostream &out) const {
    return out;
  }
};

}
