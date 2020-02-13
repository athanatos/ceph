// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore::lba_manager::btree {

/* BtreeLBAPin
 *
 * References leaf node
 */
struct BtreeLBAPin : LBAPin {
  CachedExtentRef leaf;
  paddr_t paddr;
  laddr_t laddr;
  loff_t length;
public:
  void set_paddr(paddr_t) final {}
  
  loff_t get_length() const final {
    return length;
  }
  paddr_t get_paddr() const {
    return paddr;
  }
  laddr_t get_laddr() const {
    return laddr;
  }
};

using depth_t = uint32_t;

template <typename addr_t, typename addr_off_t>
struct Node {
  depth_t depth;
  CachedExtentRef extent;
  Node(depth_t depth, CachedExtentRef extent) : depth(depth), extent(extent) {}

  using lookup_range_ertr = LBAManager::get_mapping_ertr;
  using lookup_range_ret = LBAManager::get_mapping_ret;
  virtual lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) = 0;

  virtual ~Node() = default;
};

struct LBAInternalNode : Node<laddr_t, loff_t> {
  LBAInternalNode(depth_t depth, CachedExtentRef extent)
    : Node{depth, extent} {}

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) final;

private:
  struct internal_entry_t {
    laddr_t get_laddr() const { return L_ADDR_NULL; /* TODO */ }
    loff_t get_length() const { return 0; /* TODO */ }
    paddr_t get_paddr() const { return paddr_t(); /* TODO */ }
  };
  struct internal_iterator_t {
    internal_entry_t placeholder;
    const internal_entry_t &operator*() const { return placeholder; }
    void operator++(int) {}
    void operator++() {}
    bool operator==(const internal_iterator_t &rhs) const { return true; }
  };
  std::pair<internal_iterator_t, internal_iterator_t>
  get_internal_entries(laddr_t addr, loff_t len) {
    return std::make_pair(internal_iterator_t(), internal_iterator_t());
  }
};

struct LBALeafNode : Node<laddr_t, loff_t> {
  LBALeafNode(depth_t depth, CachedExtentRef extent)
    : Node{depth, extent} {
    ceph_assert(depth == 0);
  }

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) final;
};

struct SegmentInternalNode : Node<paddr_t, segment_off_t> {
};

struct SegmentLeafNode : Node<paddr_t, segment_off_t> {
};

}
