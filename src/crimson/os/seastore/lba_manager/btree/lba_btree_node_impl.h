// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/btree_node.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"

namespace crimson::os::seastore::lba_manager::btree {

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
struct LBAInternalNode : LBANode, LBANodeIterHelper<LBAInternalNode> {
  template <typename... T>
  LBAInternalNode(T&&... t) :
    LBANode(std::forward<T>(t)...) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_INTERNAL;

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LBAInternalNode(*this));
  };

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) final;

  insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) final;

  remove_ret remove(
    Cache &cache,
    Transaction &transaction,
    laddr_t) final;

  find_hole_ret find_hole(
    Cache &cache,
    Transaction &t,
    laddr_t min,
    laddr_t max,
    loff_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(Cache &cache, Transaction &t) final {
    return do_make_split_children<LBAInternalNode, LBANode, laddr_t>(
      *this, cache, t);
  }

  LBANodeRef make_full_merge(
    Cache &cache, Transaction &t, LBANodeRef &right) final {
    return do_make_full_merge<LBAInternalNode, LBANode>(
      *this, cache, t, right);
  }

  std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_balanced(
    Cache &cache, Transaction &t,
    LBANodeRef &right, laddr_t pivot,
    bool prefer_left) final {
    return do_make_balanced<LBAInternalNode, LBANode, laddr_t>(
      *this,
      cache, t,
      right, pivot,
      prefer_left);
  }

  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    return extent_types_t::LADDR_INTERNAL;
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  void apply_delta(ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final {
    return complete_load_ertr::now();
  }

  static constexpr uint16_t CAPACITY = 255;
  static constexpr off_t SIZE_OFFSET = 0;
  static constexpr off_t LADDR_START = 16;
  static constexpr off_t PADDR_START = 2056;
  static constexpr off_t offset_of_lb(uint16_t off) {
    return LADDR_START + (off * 8);
  }
  static constexpr off_t offset_of_ub(uint16_t off) {
    return LADDR_START + ((off + 1) * 8);
  }
  static constexpr off_t offset_of_paddr(uint16_t off) {
    return PADDR_START + (off * 8);
  }

  char *get_ptr(off_t offset) {
    return get_bptr().c_str() + offset;
  }

  const char *get_ptr(off_t offset) const {
    return get_bptr().c_str() + offset;
  }

  // iterator helpers
  laddr_t get_lb(uint16_t offset) const {
    return *reinterpret_cast<const ceph_le64*>(
      get_ptr(offset_of_lb(offset)));
  }

  void set_lb(uint16_t offset, laddr_t lb) {
    *reinterpret_cast<ceph_le64*>(
      get_ptr(offset_of_lb(offset))) = lb;
  }

  paddr_t get_val(uint16_t offset) const {
    return paddr_t{
      *reinterpret_cast<const ceph_le32*>(
	get_ptr(offset_of_paddr(offset))),
	static_cast<segment_off_t>(
	  *reinterpret_cast<const ceph_les32*>(
	    get_ptr(offset_of_paddr(offset))) + 4)
	};
  }

  void set_val(uint16_t offset, paddr_t addr) {
    *reinterpret_cast<ceph_le32*>(
      get_ptr(offset_of_paddr(offset))) = addr.segment;
    *reinterpret_cast<ceph_les32*>(
      get_ptr(offset_of_paddr(offset) + 4)) = addr.offset;
  }

  char *get_key_ptr(uint16_t offset) {
    return get_ptr(offset_of_lb(offset));
  }

  char *get_val_ptr(uint16_t offset) {
    return get_ptr(offset_of_paddr(offset));
  }

  bool at_max_capacity() const final {
    return get_size() == CAPACITY;
  }

  bool at_min_capacity() const final {
    return get_size() == CAPACITY / 2;
  }

  uint16_t get_size() const {
    return *reinterpret_cast<const ceph_le16*>(get_ptr(SIZE_OFFSET));
  }

  void set_size(uint16_t size) {
    *reinterpret_cast<ceph_le16*>(get_ptr(SIZE_OFFSET)) = size;
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
  void journal_split(
    internal_iterator_t to_split,
    paddr_t new_left,
    laddr_t new_pivot,
    paddr_t new_right);
  // delta operation
  void journal_full_merge(
    internal_iterator_t left,
    paddr_t new_right);
};

/**
 * LBALeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   num_entries: uint16_t           2b
 *   (padding)  :                    14b
 *   keys       : laddr_t[170]       (170*8)b
 *   values     : lba_map_val_t[170] (170*16)b
 *                                   = 4096
 */
struct LBALeafNode : LBANode, LBANodeIterHelper<LBALeafNode> {
  template <typename... T>
  LBALeafNode(T&&... t) : LBANode(std::forward<T>(t)...) {}

  static constexpr extent_types_t type = extent_types_t::LADDR_LEAF;

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LBALeafNode(*this));
  };

  lookup_range_ret lookup_range(
    Cache &cache,
    Transaction &transaction,
    laddr_t addr,
    loff_t len) final;

  insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) final;

  remove_ret remove(
    Cache &cache,
    Transaction &transaction,
    laddr_t) final;

  find_hole_ret find_hole(
    Cache &cache,
    Transaction &t,
    laddr_t min,
    laddr_t max,
    loff_t len) final;

  std::tuple<LBANodeRef, LBANodeRef, laddr_t>
  make_split_children(Cache &cache, Transaction &t) final {
    return do_make_split_children<LBALeafNode, LBANode, laddr_t>(
      *this, cache, t);
  }

  LBANodeRef make_full_merge(
    Cache &cache, Transaction &t, LBANodeRef &right) final {
    return do_make_full_merge<LBALeafNode, LBANode>(
      *this, cache, t, right);
  }

  std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_balanced(
    Cache &cache, Transaction &t,
    LBANodeRef &right, laddr_t pivot,
    bool prefer_left) final {
    return do_make_balanced<LBALeafNode, LBANode, laddr_t>(
      *this,
      cache, t,
      right, pivot,
      prefer_left);
  }

  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    return extent_types_t::LADDR_LEAF;
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  void apply_delta(ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final {
    return complete_load_ertr::now();
  }

  // TODO
  using internal_iterator_t = node_iterator_t<LBALeafNode>;
  static constexpr uint16_t CAPACITY = 170;
  static constexpr off_t SIZE_OFFSET = 0;
  static constexpr off_t LADDR_START = 16;
  static constexpr off_t MAP_VAL_START = 1376;
  static constexpr off_t offset_of_lb(uint16_t off) {
    return LADDR_START + (off * 8);
  }
  static constexpr off_t offset_of_ub(uint16_t off) {
    return LADDR_START + ((off + 1) * 8);
  }
  static constexpr off_t offset_of_map_val(uint16_t off) {
    return MAP_VAL_START + (off * 16);
  }

  char *get_ptr(off_t offset) {
    return get_bptr().c_str() + offset;
  }

  const char *get_ptr(off_t offset) const {
    return get_bptr().c_str() + offset;
  }

  // iterator helpers
  laddr_t get_lb(uint16_t offset) const {
    return *reinterpret_cast<const ceph_le64*>(
      get_ptr(offset_of_lb(offset)));
  }

  void set_lb(uint16_t offset, laddr_t lb) {
    *reinterpret_cast<ceph_le64*>(
      get_ptr(offset_of_lb(offset))) = lb;
  }

  lba_map_val_t get_val(uint16_t offset) const {
    return lba_map_val_t{
      *reinterpret_cast<const ceph_le32*>(
	get_ptr(offset_of_map_val(offset))),
      paddr_t{
	*reinterpret_cast<const ceph_le32*>(
	  get_ptr(offset_of_map_val(offset)) + 8),
	static_cast<segment_off_t>(
	  *reinterpret_cast<const ceph_le32*>(
	    get_ptr(offset_of_map_val(offset))) + 12)
      }
    };
  }

  void set_val(uint16_t offset, lba_map_val_t addr) {
    *reinterpret_cast<ceph_le32*>(
      get_ptr(offset_of_map_val(offset))) = addr.len;
    *reinterpret_cast<ceph_le32*>(
      get_ptr(offset_of_map_val(offset)) + 8) = addr.paddr.segment;
    *reinterpret_cast<ceph_les32*>(
      get_ptr(offset_of_map_val(offset) + 12)) = addr.paddr.offset;
  }

  char *get_key_ptr(uint16_t offset) {
    return get_ptr(offset_of_lb(offset));
  }

  char *get_val_ptr(uint16_t offset) {
    return get_ptr(offset_of_map_val(offset));
  }

  bool at_max_capacity() const final {
    return get_size() == CAPACITY;
  }

  bool at_min_capacity() const final {
    return get_size() == CAPACITY / 2;
  }

  uint16_t get_size() const {
    return *reinterpret_cast<const ceph_le16*>(get_ptr(SIZE_OFFSET));
  }

  void set_size(uint16_t size) {
    *reinterpret_cast<ceph_le16*>(get_ptr(SIZE_OFFSET)) = size;
  }

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(laddr_t addr, loff_t len);

  // delta operation
  void journal_insertion(
    laddr_t laddr,
    lba_map_val_t val);

  // delta operation
  void journal_removal(
    laddr_t laddr);
};
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
    loff_t length)
    : leaf(leaf), paddr(paddr), laddr(laddr), length(length) {}

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

}
