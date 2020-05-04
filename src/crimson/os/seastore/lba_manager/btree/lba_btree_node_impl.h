// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"

#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"

namespace crimson::os::seastore::lba_manager::btree {

constexpr size_t LBA_BLOCK_SIZE = 4096;

template <typename T, typename C, typename P>
std::tuple<TCachedExtentRef<C>, TCachedExtentRef<C>, P>
do_make_balanced(
  T &left,
  Cache &cache,
  Transaction &t,
  TCachedExtentRef<C> &_right,
  bool prefer_left)
{
  ceph_assert(_right->get_type() == T::type);
  T &right = *static_cast<T*>(_right.get());
  auto replacement_left = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);
  auto replacement_right = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);

  auto total = left.get_size() + right.get_size();
  auto pivot_idx = (left.get_size() + right.get_size()) / 2;
  if (total % 2 && prefer_left) {
    pivot_idx++;
  }
  auto replacement_pivot = pivot_idx > left.get_size() ?
    right.iter_idx(pivot_idx - left.get_size())->get_lb() :
    left.iter_idx(pivot_idx)->get_lb();

  if (pivot_idx < left.get_size()) {
    replacement_left->copy_from_foreign(
      replacement_left->end(),
      left.begin(),
      left.iter_idx(pivot_idx));
    replacement_left->set_size(pivot_idx);

    replacement_right->copy_from_foreign(
      replacement_right->end(),
      left.iter_idx(pivot_idx),
      left.end());

    replacement_right->set_size(left.get_size() - pivot_idx);
    replacement_right->copy_from_foreign(
      replacement_right->end(),
      right.begin(),
      right.end());
    replacement_right->set_size(total - pivot_idx);
  } else {
    replacement_left->copy_from_foreign(
      replacement_left->end(),
      left.begin(),
      left.end());
    replacement_left->set_size(left.get_size());

    replacement_left->copy_from_foreign(
      replacement_left->end(),
      right.begin(),
      right.iter_idx(pivot_idx - left.get_size()));
    replacement_left->set_size(pivot_idx);

    replacement_right->copy_from_foreign(
      replacement_right->end(),
      right.iter_idx(pivot_idx - left.get_size()),
      right.end());
    replacement_right->set_size(total - pivot_idx);
  }

  return std::make_tuple(
    replacement_left,
    replacement_right,
    replacement_pivot);
}


/**
 * LBAInternalNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   num_entries: uint16_t         2b
 *   (padding)  :                  14b
 *   keys       : laddr_t[255]     (255*8)b
 *   values     : paddr_t[255]     (255*8)b
 *                                 = 4096
 */
constexpr size_t INTERNAL_NODE_CAPACITY = 255;
struct LBAInternalNode
  : LBANode,
    common::FixedKVNodeLayout<
      INTERNAL_NODE_CAPACITY,
      laddr_t, laddr_le_t,
      paddr_t, paddr_le_t> {
  using internal_iterator_t = fixed_node_iter_t;
  template <typename... T>
  LBAInternalNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_INTERNAL;

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LBAInternalNode(*this));
  };

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    extent_len_t len) final;

  insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) final;

  mutate_mapping_ret mutate_mapping(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    mutate_func_t &&f) final;

  find_hole_ret find_hole(
    Cache &cache,
    Transaction &t,
    laddr_t min,
    laddr_t max,
    extent_len_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(Cache &cache, Transaction &t) final {
    auto left = cache.alloc_new_extent<LBAInternalNode>(
      t, LBA_BLOCK_SIZE);
    auto right = cache.alloc_new_extent<LBAInternalNode>(
      t, LBA_BLOCK_SIZE);
    return std::make_tuple(
      left,
      right,
      split_into(*left, *right));
  }

  LBANodeRef make_full_merge(
    Cache &cache, Transaction &t, LBANodeRef &right) final {
    auto replacement = cache.alloc_new_extent<LBAInternalNode>(
      t, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBAInternalNode>());
    return replacement;
  }

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_balanced(
    Cache &cache, Transaction &t,
    LBANodeRef &_right,
    bool prefer_left) final {
    ceph_assert(_right->get_type() == type);
    auto &right = *_right->cast<LBAInternalNode>();
    auto replacement_left = cache.alloc_new_extent<LBAInternalNode>(
      t, LBA_BLOCK_SIZE);
    auto replacement_right = cache.alloc_new_extent<LBAInternalNode>(
      t, LBA_BLOCK_SIZE);

    return std::make_tuple(
      replacement_left,
      replacement_right,
      balance_into_new_nodes(
	*this,
	right,
	prefer_left,
	*replacement_left,
	*replacement_right));
  }

  void resolve_relative_addrs(paddr_t base);

  void on_delta_write(paddr_t record_block_offset) final {
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    resolve_relative_addrs(get_paddr());
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  ceph::bufferlist get_delta() final {
    // TODO
    return ceph::bufferlist();
  }

  void apply_delta(paddr_t delta_base, ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const {
    return get_size() == get_capacity() / 2;
  }

  complete_load_ertr::future<> complete_load() final {
    resolve_relative_addrs(get_paddr());
    return complete_load_ertr::now();
  }

  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_ub() > l)
	break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_lb() >= r)
	break;
    }
    return std::make_pair(retl, retr);
  }

  using split_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using split_ret = split_ertr::future<LBANodeRef>;
  split_ret split_entry(
    Cache &c, Transaction &t, laddr_t addr,
    internal_iterator_t,
    LBANodeRef entry);

  using merge_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using merge_ret = merge_ertr::future<LBANodeRef>;
  merge_ret merge_entry(
    Cache &c, Transaction &t, laddr_t addr,
    internal_iterator_t,
    LBANodeRef entry);

  internal_iterator_t get_containing_child(laddr_t laddr);

  // delta operation
  void journal_remove(
    laddr_t to_remove);
  void journal_insert(
    laddr_t to_insert,
    paddr_t val);
};

/**
 * LBALeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   num_entries: uint16_t           2b
 *   (padding)  :                    6b
 *   keys       : laddr_t[170]       (146*8)b
 *   values     : lba_map_val_t[170] (146*20)b
 *                                   = 4096
 */
constexpr size_t LEAF_NODE_CAPACITY = 146;
struct lba_map_val_le_t {
  extent_len_le_t len = extent_len_le_t(0);
  paddr_le_t paddr;
  ceph_le32 refcount = ceph_le32(0);
  ceph_le32 checksum = ceph_le32(0);

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(extent_len_le_t(val.len)),
      paddr(paddr_le_t(val.paddr)),
      refcount(ceph_le32(val.refcount)),
      checksum(ceph_le32(val.checksum)) {}

  operator lba_map_val_t() const {
    return lba_map_val_t{ len, paddr, refcount, checksum };
  }
};

struct LBALeafNode
  : LBANode,
    common::FixedKVNodeLayout<
      LEAF_NODE_CAPACITY,
      laddr_t, laddr_le_t,
      lba_map_val_t, lba_map_val_le_t> {
  using internal_iterator_t = fixed_node_iter_t;
  template <typename... T>
  LBALeafNode(T&&... t) :
    LBANode(std::forward<T>(t)...),
    FixedKVNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_LEAF;

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LBALeafNode(*this));
  };

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    extent_len_t len) final;

  insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) final;

  mutate_mapping_ret mutate_mapping(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    mutate_func_t &&f) final;

  find_hole_ret find_hole(
    Cache &cache,
    Transaction &t,
    laddr_t min,
    laddr_t max,
    extent_len_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(Cache &cache, Transaction &t) final {
    auto left = cache.alloc_new_extent<LBALeafNode>(
      t, LBA_BLOCK_SIZE);
    auto right = cache.alloc_new_extent<LBALeafNode>(
      t, LBA_BLOCK_SIZE);
    return std::make_tuple(
      left,
      right,
      split_into(*left, *right));
  }

  LBANodeRef make_full_merge(
    Cache &cache, Transaction &t, LBANodeRef &right) final {
    auto replacement = cache.alloc_new_extent<LBALeafNode>(
      t, LBA_BLOCK_SIZE);
    replacement->merge_from(*this, *right->cast<LBALeafNode>());
    return replacement;
  }

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_balanced(
    Cache &cache, Transaction &t,
    LBANodeRef &_right,
    bool prefer_left) final {
    ceph_assert(_right->get_type() == type);
    auto &right = *_right->cast<LBALeafNode>();
    auto replacement_left = cache.alloc_new_extent<LBALeafNode>(
      t, LBA_BLOCK_SIZE);
    auto replacement_right = cache.alloc_new_extent<LBALeafNode>(
      t, LBA_BLOCK_SIZE);
    return std::make_tuple(
      replacement_left,
      replacement_right,
      balance_into_new_nodes(
	*this,
	right,
	prefer_left,
	*replacement_left,
	*replacement_right));
  }

  void resolve_relative_addrs(paddr_t base);

  void on_delta_write(paddr_t record_block_offset) final {
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    resolve_relative_addrs(get_paddr());
  }

  ceph::bufferlist get_delta() final {
    // TODO
    return ceph::bufferlist();
  }

  void apply_delta(paddr_t delta_base, ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final {
    resolve_relative_addrs(get_paddr());
    return complete_load_ertr::now();
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  bool at_max_capacity() const final {
    return get_size() == get_capacity();
  }

  bool at_min_capacity() const final {
    return get_size() == get_capacity();
  }

  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_lb() >= l || (retl->get_lb() + retl->get_val().len) > l)
	break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_lb() >= r)
	break;
    }
    return std::make_pair(retl, retr);
  }
  internal_iterator_t upper_bound(laddr_t l) {
    auto ret = begin();
    for (; ret != end(); ++ret) {
      if (ret->get_lb() > l)
	break;
    }
    return ret;
  }

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(laddr_t addr, extent_len_t len);

  // delta operations
  void journal_mutated(
    laddr_t laddr,
    lba_map_val_t val);
  void journal_insertion(
    laddr_t laddr,
    lba_map_val_t val);
  void journal_removal(
    laddr_t laddr);
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

/* BtreeLBAPin
 *
 * References leaf node
 *
 * TODO: does not at this time actually keep the relevant
 * leaf resident in memory.  This is actually a bit tricky
 * as we can mutate and therefore replace a leaf referenced
 * by other, uninvolved but cached extents.  Will need to
 * come up with some kind of pinning mechanism that handles
 * that well.
 */
struct BtreeLBAPin : LBAPin {
  paddr_t paddr;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t length = 0;
  unsigned refcount = 0;

public:
  BtreeLBAPin(
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t length,
    unsigned refcount)
    : paddr(paddr), laddr(laddr), length(length), refcount(refcount) {}

  extent_len_t get_length() const final {
    return length;
  }
  paddr_t get_paddr() const final {
    return paddr;
  }
  laddr_t get_laddr() const final {
    return laddr;
  }
  unsigned get_refcount() const final {
    return refcount;
  }
};

}
