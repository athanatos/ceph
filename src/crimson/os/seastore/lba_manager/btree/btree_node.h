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

class LBABtreeCachedExtent : public CachedExtent {
protected:
  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new LBABtreeCachedExtent(*this));
  };

  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    ceph_assert(0 == "TODO");
    return extent_types_t::LBA_BLOCK;
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  void apply_delta(ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final {
    ceph_assert(0 == "TODO");
    return complete_load_ertr::now();
  }

public:
  LBABtreeCachedExtent(
    ceph::bufferptr &&ptr) : CachedExtent(std::move(ptr)) {}
};
using LBABtreeCachedExtentRef = TCachedExtentRef<LBABtreeCachedExtent>;

/* BtreeLBAPin
 *
 * References leaf node
 */
struct BtreeLBAPin : LBAPin {
  LBABtreeCachedExtentRef leaf;
  paddr_t paddr;
  laddr_t laddr;
  loff_t length;
public:
  BtreeLBAPin(
    LBABtreeCachedExtentRef leaf,
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

using depth_t = uint32_t;

template <typename addr_t, typename addr_off_t>
struct Node {
  depth_t depth;
  LBABtreeCachedExtentRef extent;
  Node(depth_t depth, LBABtreeCachedExtentRef extent) : depth(depth), extent(extent) {}

  virtual ~Node() = default;
};

struct lba_map_val_t {
  loff_t len = 0;
  paddr_t paddr;
  // other stuff: checksum, refcount
};

struct LBANode : Node<laddr_t, loff_t> {
  using lookup_range_ertr = LBAManager::get_mapping_ertr;
  using lookup_range_ret = LBAManager::get_mapping_ret;

  LBANode(depth_t depth, LBABtreeCachedExtentRef extent) : Node(depth, extent) {}

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
  using insert_ret = insert_ertr::future<>;
  virtual insert_ret insert(
    Cache &cache,
    Transaction &transaction,
    laddr_t laddr,
    lba_map_val_t val) = 0;

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


  static std::unique_ptr<LBANode> get_node(
    depth_t depth,
    LBABtreeCachedExtentRef extent);

  virtual ~LBANode() = default;
};
using LBANodeRef = std::unique_ptr<LBANode>;

struct LBAInternalNode : LBANode {
  LBAInternalNode(depth_t depth, LBABtreeCachedExtentRef extent)
    : LBANode(depth, extent) {}

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

  bool at_max_capacity() const final { return false; /* TODO */ }
  bool at_min_capacity() const final { return false; /* TODO */ }

private:
  struct internal_entry_t {
    laddr_t get_laddr() const { return L_ADDR_NULL; /* TODO */ }
    loff_t get_length() const { return 0; /* TODO */ }
    paddr_t get_paddr() const { return paddr_t(); /* TODO */ }
  };
  struct internal_iterator_t {
    internal_entry_t placeholder;
    const internal_entry_t &operator*() const { return placeholder; }
    const internal_entry_t *operator->() const { return &placeholder; }
    void operator++(int) {}
    void operator++() {}
    bool operator==(const internal_iterator_t &rhs) const { return true; }
  };

  using split_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using split_ret = split_ertr::future<LBANodeRef>;
  split_ret split_entry(Cache &c, Transaction &t, laddr_t addr,
			internal_iterator_t&);

  internal_iterator_t get_insertion_point(laddr_t laddr);
  
  std::pair<internal_iterator_t, internal_iterator_t>
  get_internal_entries(laddr_t addr, loff_t len);
};

struct LBALeafNode : LBANode {
  LBALeafNode(depth_t depth, LBABtreeCachedExtentRef extent)
    : LBANode(depth, extent) {
    ceph_assert(depth == 0);
  }

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

  bool at_max_capacity() const final { return false; /* TODO */ }
  bool at_min_capacity() const final { return false; /* TODO */ }

private:
  struct internal_entry_t {
    paddr_t get_paddr() const { return paddr_t(); /* TODO */ }
    laddr_t get_laddr() const { return L_ADDR_NULL; /* TODO */ }
    loff_t get_length() const { return 0; /* TODO */ }
  };
  struct internal_iterator_t {
    internal_entry_t placeholder;
    const internal_entry_t &operator*() const { return placeholder; }
    void operator++(int) {}
    void operator++() {}
    bool operator==(const internal_iterator_t &rhs) const { return true; }
    bool operator!=(const internal_iterator_t &rhs) const {
      return !(*this == rhs);
    }
  };
  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(laddr_t addr, loff_t len);
};

Cache::get_extent_ertr::future<LBABtreeCachedExtentRef> get_lba_btree_extent(
  Cache &cache,
  Transaction &t,
  paddr_t offset) {
  return cache.get_extent<LBABtreeCachedExtent>(
    t,
    offset,
    LBA_BLOCK_SIZE);
}

struct SegmentInternalNode : Node<paddr_t, segment_off_t> {
};

struct SegmentLeafNode : Node<paddr_t, segment_off_t> {
};

}
