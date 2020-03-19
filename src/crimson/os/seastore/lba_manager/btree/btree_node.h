// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore::lba_manager::btree {

constexpr segment_off_t LBA_BLOCK_SIZE = 4096; // TODO

using depth_t = uint32_t;

template <typename addr_t, typename addr_off_t>
struct Node : public CachedExtent {
  depth_t depth;

  template <typename... T>
  Node(T&&... t) : CachedExtent(std::forward<T>(t)...) {}

  void set_depth(depth_t _depth) { depth = _depth; }

  virtual ~Node() = default;
};

struct lba_map_val_t {
  loff_t len = 0;
  paddr_t paddr;
  // other stuff: checksum, refcount
};

class BtreeLBAPin;
using BtreeLBAPinRef = std::unique_ptr<BtreeLBAPin>;

struct LBANode : Node<laddr_t, loff_t> {
  using lookup_range_ertr = LBAManager::get_mapping_ertr;
  using lookup_range_ret = LBAManager::get_mapping_ret;

  template <typename... T>
  LBANode(T&&... t) : Node(std::forward<T>(t)...) {}

  virtual lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) = 0;

  /**
   * Precondition: !at_max_capacity()
   */
  using insert_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using insert_ret = insert_ertr::future<LBAPinRef>;
  virtual insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) = 0;

  /**
   * Finds minimum hole greater in [min, max) of size at least len
   *
   * Returns L_ADDR_NULL if unfound
   */
  using find_hole_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using find_hole_ret = find_hole_ertr::future<laddr_t>;
  virtual find_hole_ret find_hole(
    Cache &cache,
    Transaction &t,
    laddr_t min,
    laddr_t max,
    loff_t len) = 0;

  /**
   * Precondition: !at_min_capacity()
   */
  using remove_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using remove_ret = remove_ertr::future<>;
  virtual remove_ret remove(
    Cache &cache,
    Transaction &transaction,
    laddr_t) = 0;
  

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;


  virtual ~LBANode() = default;
};
using LBANodeRef = TCachedExtentRef<LBANode>;

struct LBALeafNode;
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

/* BtreeLBAPin
 *
 * References leaf node
 */
struct BtreeLBAPin : LBAPin {
  LBALeafNodeRef leaf;
  paddr_t paddr;
  laddr_t laddr;
  loff_t length;
public:
  BtreeLBAPin(
    LBALeafNodeRef leaf,
    paddr_t paddr,
    laddr_t laddr,
    loff_t length);

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

Cache::get_extent_ertr::future<LBANodeRef> get_lba_btree_extent(
  Cache &cache,
  Transaction &t,
  depth_t depth,
  paddr_t offset);

struct SegmentInternalNode : Node<paddr_t, segment_off_t> {
};

struct SegmentLeafNode : Node<paddr_t, segment_off_t> {
};

}
