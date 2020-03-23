// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>

#include "include/buffer.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/lba_manager/btree/btree_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

/**
 * LBAInternalNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   start_pivot: laddr_t          8b
 *   num_entries: uint16_t         2b
 *   (padding)  :                  6b
 *   keys       : laddr_t[255]     (255*8)b
 *   values     : paddr_t[255]     (255*8)b
 *                                 = 4096
 */
struct LBAInternalNode : LBANode {
  template <typename... T>
  LBAInternalNode(T&&... t) : LBANode(std::forward<T>(t)...) {}

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

  bool at_max_capacity() const final { return false; /* TODO */ }
  bool at_min_capacity() const final { return false; /* TODO */ }

protected:
  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    ceph_assert(0 == "TODO");
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
    ceph_assert(0 == "TODO");
    return complete_load_ertr::now();
  }

private:
  static constexpr uint16_t CAPACITY = 254;
  static constexpr off_t LADDR_START = 16;
  static constexpr off_t PADDR_START = 16;
  static constexpr off_t offset_of_lb(uint16_t off) {
    return LADDR_START + (off * 8);
  }
  static constexpr off_t offset_of_ub(uint16_t off) {
    return LADDR_START + ((off + 1) * 8);
  }
  static constexpr off_t offset_of_paddr(uint16_t off) {
    return PADDR_START + (off * 8);
  }

  struct internal_iterator_t {
    LBAInternalNode *node;
    uint16_t offset;
    internal_iterator_t(
      LBAInternalNode *parent,
      uint16_t offset) : node(parent), offset(offset) {}

    internal_iterator_t(const internal_iterator_t &) = default;
    internal_iterator_t(internal_iterator_t &&) = default;
    internal_iterator_t &operator=(const internal_iterator_t &) = default;
    internal_iterator_t &operator=(internal_iterator_t &&) = default;

    internal_iterator_t &operator*() { return *this; }
    internal_iterator_t *operator->() { return this; }

    internal_iterator_t operator++(int) {
      auto ret = *this;
      ++offset;
      return ret;
    }

    internal_iterator_t &operator++() {
      ++offset;
      return *this;
    }

    bool operator==(const internal_iterator_t &rhs) const {
      ceph_assert(node == rhs.node);
      return rhs.offset == offset;
    }

    bool operator!=(const internal_iterator_t &rhs) const {
      return !(*this == rhs);
    }

    laddr_t get_lb() const {
      laddr_t ret;
      bufferlist bl;
      bl.append(node->get_bptr());
      auto lbptr = bl.cbegin(offset_of_lb(offset));
      ::decode(ret, lbptr);
      return ret;
    }

    laddr_t get_ub() const {
      laddr_t ret;
      bufferlist bl;
      bl.append(node->get_bptr());
      auto ubptr = bl.cbegin(offset_of_ub(offset));
      ::decode(ret, ubptr);
      return ret;
    }

    paddr_t get_paddr() const {
      paddr_t ret;
      bufferlist bl;
      bl.append(node->get_bptr());
      auto ptr = bl.cbegin(offset_of_paddr(offset));
      ::decode(ret, ptr);
      return ret;
    }
    loff_t get_length() const {
      return 0;
    }
    bool contains(laddr_t addr) {
      return (get_lb() <= addr) && (get_ub() > addr);
    }
  };
  internal_iterator_t begin() {
    return internal_iterator_t(this, 0);
  }
  internal_iterator_t end() {
    return internal_iterator_t(this, CAPACITY+1);
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
  
  std::pair<internal_iterator_t, internal_iterator_t>
  get_internal_entries(laddr_t addr, loff_t len);
};


LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  loff_t len)
{
  auto [begin, end] = get_internal_entries(addr, len);
  auto result_up = std::make_unique<lba_pin_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &cache, &t, &result, addr, len](const auto &val) mutable {
      return get_lba_btree_extent(
	cache,
	t,
	depth-1,
	val.get_paddr()).safe_then(
	  [this, &cache, &t, &result, addr, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return extent->lookup_range(
	      cache,
	      t,
	      addr,
	      len).safe_then(
		[&cache, &t, &result, addr, len](auto pin_list) mutable {
		  result.splice(result.end(), pin_list,
				pin_list.begin(), pin_list.end());
		});
	  });
    }).safe_then([result=std::move(result_up)] {
      return lookup_range_ertr::make_ready_future<lba_pin_list_t>(
	std::move(*result));
    });
}

LBAInternalNode::insert_ret LBAInternalNode::insert(
  Cache &cache,
  Transaction &t,
  laddr_t laddr,
  lba_map_val_t val)
{
  auto insertion_pt = get_containing_child(laddr);
  return get_lba_btree_extent(
    cache,
    t,
    depth-1,
    insertion_pt->get_paddr()).safe_then(
      [this, insertion_pt, &cache, &t, laddr, val=std::move(val)](
	auto extent) mutable {
	return extent->at_max_capacity() ?
	  split_entry(cache, t, laddr, insertion_pt, extent) :
	  insert_ertr::make_ready_future<LBANodeRef>(std::move(extent));
      }).safe_then([this, &cache, &t, laddr, val=std::move(val)](
		     auto extent) mutable {
	return extent->insert(cache, t, laddr, val);
      });
}

LBAInternalNode::remove_ret LBAInternalNode::remove(
  Cache &cache,
  Transaction &t,
  laddr_t laddr)
{
  auto removal_pt = get_containing_child(laddr);
  return get_lba_btree_extent(
    cache,
    t,
    depth-1,
    removal_pt->get_paddr()
  ).safe_then([this, removal_pt, &cache, &t, laddr](auto extent) {
    return extent->at_min_capacity() ?
      merge_entry(cache, t, laddr, removal_pt, extent) :
      remove_ertr::make_ready_future<LBANodeRef>(std::move(extent));
  }).safe_then([&cache, &t, laddr](auto extent) {
    return extent->remove(cache, t, laddr);
  });
}

LBAInternalNode::find_hole_ret LBAInternalNode::find_hole(
  Cache &cache,
  Transaction &t,
  laddr_t min,
  laddr_t max,
  loff_t len)
{
  return find_hole_ret(
    find_hole_ertr::ready_future_marker{},
    L_ADDR_MAX);
}

LBAInternalNode::split_ret
LBAInternalNode::split_entry(
  Cache &c, Transaction &t, laddr_t addr, internal_iterator_t, LBANodeRef entry)
{
  return split_ertr::make_ready_future<LBANodeRef>();
}

LBAInternalNode::merge_ret
LBAInternalNode::merge_entry(
  Cache &c, Transaction &t, laddr_t addr, internal_iterator_t, LBANodeRef entry)
{
  return split_ertr::make_ready_future<LBANodeRef>();
}


LBAInternalNode::internal_iterator_t
LBAInternalNode::get_containing_child(laddr_t laddr)
{
  // TODO: binary search
  for (auto i = begin(); i != end(); ++i) {
    if (i.contains(laddr))
      return i;
  }
  ceph_assert(0 == "invalid");
  return end();
}

std::pair<LBAInternalNode::internal_iterator_t,
	  LBAInternalNode::internal_iterator_t>
LBAInternalNode::get_internal_entries(laddr_t addr, loff_t len)
{
  return std::make_pair(
    internal_iterator_t(this, 0),
    internal_iterator_t(this, 0));
}

/**
 * LBALeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * LBA Tree.
 *
 * Layout (4k):
 *   start_pivot: laddr_t            8b
 *   num_entries: uint16_t           2b
 *   (padding)  :                    6b
 *   keys       : laddr_t[170]       (170*8)b
 *   values     : lba_map_val_t[170] (170*16)b
 *                                   = 4096
 */
struct LBALeafNode : LBANode {
  template <typename... T>
  LBALeafNode(T&&... t) : LBANode(std::forward<T>(t)...) {}

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

  bool at_max_capacity() const final { return false; /* TODO */ }
  bool at_min_capacity() const final { return false; /* TODO */ }

protected:
  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    ceph_assert(0 == "TODO");
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
    ceph_assert(0 == "TODO");
    return complete_load_ertr::now();
  }

private:
  struct internal_entry_t {
    paddr_t get_paddr() const { return paddr_t(); /* TODO */ }
    laddr_t get_laddr() const { return L_ADDR_NULL; /* TODO */ }
    loff_t get_length() const { return 0; /* TODO */ }
  };
  struct internal_iterator_t {
    internal_entry_t placeholder;
    const internal_entry_t &operator*() const { return placeholder; }
    const internal_entry_t *operator->() const { return &placeholder; }
    void operator++(int) {}
    void operator++() {}
    bool operator==(const internal_iterator_t &rhs) const { return true; }
    bool operator!=(const internal_iterator_t &rhs) const { return !(*this == rhs); }
  };
  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(laddr_t addr, loff_t len);
};
using LBALeafNodeRef = TCachedExtentRef<LBALeafNode>;

LBALeafNode::lookup_range_ret LBALeafNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  loff_t len)
{
  auto ret = lba_pin_list_t();
  auto [i, end] = get_leaf_entries(addr, len);
  for (; i != end; ++i) {
    ret.emplace_back(
      std::make_unique<BtreeLBAPin>(
	LBALeafNodeRef(this),
	(*i).get_paddr(),
	(*i).get_laddr(),
	(*i).get_length()));
  }
  return lookup_range_ertr::make_ready_future<lba_pin_list_t>(
    std::move(ret));
}

LBALeafNode::insert_ret LBALeafNode::insert(
  Cache &cache,
  Transaction &transaction,
  laddr_t laddr,
  lba_map_val_t val)
{
  ceph_assert(!at_max_capacity());
  /* Mutate the contents generating a delta if dirty rather than pending */
  /* If dirty, do the thing that causes the extent to be fixed-up once
   * committed */
  return insert_ret(
    insert_ertr::ready_future_marker{},
    LBAPinRef());
}

LBALeafNode::remove_ret LBALeafNode::remove(
  Cache &cache,
  Transaction &transaction,
  laddr_t)
{
  ceph_assert(!at_min_capacity());
  /* Mutate the contents generating a delta if dirty rather than pending */
  /* If dirty, do the thing that causes the extent to be fixed-up once
   * committed */
  return insert_ertr::now();
}

LBALeafNode::find_hole_ret LBALeafNode::find_hole(
  Cache &cache,
  Transaction &t,
  laddr_t min,
  laddr_t max,
  loff_t len)
{
  return find_hole_ret(
    find_hole_ertr::ready_future_marker{},
    L_ADDR_MAX);
}

std::pair<LBALeafNode::internal_iterator_t, LBALeafNode::internal_iterator_t>
LBALeafNode::get_leaf_entries(laddr_t addr, loff_t len)
{
  return std::make_pair(internal_iterator_t(), internal_iterator_t());
}

Cache::get_extent_ertr::future<LBANodeRef> get_lba_btree_extent(
  Cache &cache,
  Transaction &t,
  depth_t depth,
  paddr_t offset) {
  if (depth > 0) {
   return cache.get_extent<LBAInternalNode>(
      t,
      offset,
      LBA_BLOCK_SIZE).safe_then([](auto ret) {
	return LBANodeRef(ret.detach());
      });
    
  } else {
    return cache.get_extent<LBALeafNode>(
      t,
      offset,
      LBA_BLOCK_SIZE).safe_then([](auto ret) {
	return LBANodeRef(ret.detach());
      });
  }
}


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
