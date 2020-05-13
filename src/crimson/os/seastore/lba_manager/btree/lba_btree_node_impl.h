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

  void resolve_relative_addrs(paddr_t base);

  void on_delta_write(paddr_t record_block_offset) final {
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    resolve_relative_addrs(get_paddr());
  }

  extent_types_t get_type() const final {
    return extent_types_t::LADDR_INTERNAL;
  }

  std::ostream &print_detail(std::ostream &out) const final;

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
      *reinterpret_cast<const ceph_les32*>(
	get_ptr(offset_of_paddr(offset)) + 4)
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
 *   (padding)  :                    30b
 *   keys       : laddr_t[170]       (127*8)b
 *   values     : lba_map_val_t[170] (127*24)b
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
    return extent_types_t::LADDR_LEAF;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  // TODO
  using internal_iterator_t = node_iterator_t<LBALeafNode>;
  static constexpr uint16_t CAPACITY = 127;
  static constexpr off_t SIZE_OFFSET = 0;
  static constexpr off_t LADDR_START = 32;
  static constexpr off_t MAP_VAL_START = 1048;
  static constexpr off_t offset_of_lb(uint16_t off) {
    return LADDR_START + (off * 8);
  }
  static constexpr off_t offset_of_ub(uint16_t off) {
    return LADDR_START + ((off + 1) * 8);
  }
  static constexpr off_t offset_of_map_val(uint16_t off) {
    return MAP_VAL_START + (off * 24);
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

  extent_len_t get_length(uint16_t offset) const {
    return *reinterpret_cast<const ceph_le32*>(
      get_ptr(offset_of_map_val(offset)));
  }

  uint32_t get_refcount(uint16_t offset) const {
    return *reinterpret_cast<const ceph_le32*>(
      get_ptr(offset_of_map_val(offset) + 16));
  }

  uint32_t get_checksum(uint16_t offset) const {
    return *reinterpret_cast<const ceph_le32*>(
      get_ptr(offset_of_map_val(offset) + 20));
  }

  lba_map_val_t get_val(uint16_t offset) const {
    return lba_map_val_t{
      get_length(offset),
      paddr_t{
	*reinterpret_cast<const ceph_le32*>(
	  get_ptr(offset_of_map_val(offset)) + 8),
	*reinterpret_cast<const ceph_les32*>(
	  get_ptr(offset_of_map_val(offset)) + 12)
      },
      get_refcount(offset),
      get_checksum(offset)
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

  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_lb() >= l || (retl->get_lb() + retl->get_length()) > l)
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
