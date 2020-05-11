// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "crimson/common/log.h"

namespace crimson::os::seastore::lba_manager::btree {

struct lba_map_val_t {
  extent_len_t len = 0;
  paddr_t paddr;
  // other stuff: checksum, refcount
};

class BtreeLBAPin;
using BtreeLBAPinRef = std::unique_ptr<BtreeLBAPin>;

struct LBANode : CachedExtent {
  using LBANodeRef = TCachedExtentRef<LBANode>;
  using lookup_range_ertr = LBAManager::get_mapping_ertr;
  using lookup_range_ret = LBAManager::get_mapping_ret;

  template <typename... T>
  LBANode(T&&... t) : CachedExtent(std::forward<T>(t)...) {}

  LBANode(const LBANode &rhs) = default;

  depth_t depth = 0;
  void set_depth(depth_t _depth) { depth = _depth; }

  virtual lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    extent_len_t len) = 0;

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
    extent_len_t len) = 0;

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

  virtual std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_split_children(Cache &cache, Transaction &t) = 0;

  virtual LBANodeRef make_full_merge(
    Cache &cache, Transaction &t, LBANodeRef &right) = 0;

  virtual std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_balanced(
    Cache &cache, Transaction &t, LBANodeRef &right,
    laddr_t pivot, bool prefer_left) = 0;

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;

  virtual ~LBANode() = default;
};
using LBANodeRef = LBANode::LBANodeRef;

Cache::get_extent_ertr::future<LBANodeRef> get_lba_btree_extent(
  Cache &cache,
  Transaction &t,
  depth_t depth,
  paddr_t offset,
  paddr_t base);

}
