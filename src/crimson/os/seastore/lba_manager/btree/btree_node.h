// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

template <typename addr_t, typename addr_off_t>
struct Node {
  CachedExtentRef extent;

  virtual addr_t get_next(addr_t addr) { return addr_t{}; }
  virtual ~Node() {}
};

struct LBARootNode : Node<laddr_t, loff_t> {
};

struct LBAInternalNode : Node<laddr_t, loff_t> {
};

struct LBALeafNode : Node<laddr_t, loff_t> {
};

struct SegmentRootNode : Node<paddr_t, segment_off_t> {
};

struct SegmentInternalNode : Node<paddr_t, segment_off_t> {
};

struct SegmentLeafNode : Node<paddr_t, segment_off_t> {
};

}
