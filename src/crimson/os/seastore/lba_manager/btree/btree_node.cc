// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

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

  std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_split_children(Cache &cache, Transaction &t) final;

  bool at_max_capacity() const final { return get_size() == CAPACITY; }
  bool at_min_capacity() const final { return get_size() == CAPACITY / 2; }

protected:
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
    ceph_assert(0 == "TODO");
    return complete_load_ertr::now();
  }

private:
  static constexpr uint16_t CAPACITY = 254;
  static constexpr off_t PIVOT_OFFSET = 0;
  static constexpr off_t SIZE_OFFSET = 8;
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

  char *get_ptr(off_t offset) {
    return get_bptr().c_str() + offset;
  }

  const char *get_ptr(off_t offset) const {
    return get_bptr().c_str() + offset;
  }

  laddr_t get_pivot() const {
    return *reinterpret_cast<const ceph_le64*>(get_ptr(PIVOT_OFFSET));
  }

  void set_pivot(laddr_t pivot) {
    *reinterpret_cast<ceph_le64*>(get_ptr(PIVOT_OFFSET)) = pivot;
  }

  uint16_t get_size() const {
    return *reinterpret_cast<const ceph_le16*>(get_ptr(SIZE_OFFSET));
  }

  void set_size(uint16_t size) {
    *reinterpret_cast<ceph_le16*>(get_ptr(SIZE_OFFSET)) = size;
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

    uint16_t operator-(const internal_iterator_t &rhs) const {
      ceph_assert(rhs.node == node);
      return offset - rhs.offset;
    }

    bool operator==(const internal_iterator_t &rhs) const {
      ceph_assert(node == rhs.node);
      return rhs.offset == offset;
    }

    bool operator!=(const internal_iterator_t &rhs) const {
      return !(*this == rhs);
    }

    laddr_t get_lb() const {
      return *reinterpret_cast<const ceph_le64*>(
	node->get_ptr(offset_of_lb(offset)));
    }

    void set_lb(laddr_t lb) {
      *reinterpret_cast<ceph_le64*>(
	node->get_ptr(offset_of_lb(offset))) = lb;
    }

    laddr_t get_ub() const {
      return *reinterpret_cast<const ceph_le64*>(
	node->get_ptr(offset_of_ub(offset)));
    }

    void set_ub(laddr_t ub) {
      *reinterpret_cast<ceph_le64*>(
	node->get_ptr(offset_of_ub(offset))) = ub;
    }

    paddr_t get_paddr() const {
      return paddr_t{
	*reinterpret_cast<const ceph_les32*>(
	  node->get_ptr(offset_of_paddr(offset))),
	static_cast<segment_off_t>(
	  *reinterpret_cast<const ceph_les32*>(
	    node->get_ptr(offset_of_paddr(offset))) + 4)
	  };
    };

    void set_paddr(paddr_t addr) {
      *reinterpret_cast<ceph_le32*>(
	node->get_ptr(offset_of_paddr(offset))) = addr.segment;
      *reinterpret_cast<ceph_le32*>(
	node->get_ptr(offset_of_paddr(offset) + 4)) = addr.offset;
    }

    bool contains(laddr_t addr) {
      return (get_lb() <= addr) && (get_ub() > addr);
    }

    char *get_begin_ptr() {
      return node->get_ptr(offset_of_lb(offset));
    }
  };
  
  internal_iterator_t begin() {
    return internal_iterator_t(this, 0);
  }
  internal_iterator_t end() {
    return internal_iterator_t(this, CAPACITY+1);
  }
  std::pair<internal_iterator_t, internal_iterator_t> bound(
    laddr_t l, laddr_t r) {
    auto retl = begin();
    for (; retl != end(); ++retl) {
      if (retl->get_lb() <= l && retl->get_ub() > l)
	break;
    }
    auto retr = retl;
    for (; retr != end(); ++retr) {
      if (retr->get_lb() > r)
	break;
    }
    return std::make_pair(retl, retr);
  }
  internal_iterator_t get_split_pivot() {
    return internal_iterator_t(
      this,
      get_size() / 2);
  }

  void copy_from_foreign(
    internal_iterator_t tgt,
    internal_iterator_t from_src,
    internal_iterator_t to_src) {
    ceph_assert(tgt->node != from_src->node);
    ceph_assert(to_src->node == from_src->node);
    memcpy(tgt->get_begin_ptr(), from_src->get_begin_ptr(), to_src - from_src);
  }

  void copy_from_local(
    internal_iterator_t tgt,
    internal_iterator_t from_src,
    internal_iterator_t to_src) {
    ceph_assert(tgt->node == from_src->node);
    ceph_assert(to_src->node == from_src->node);
    memmove(tgt->get_begin_ptr(), from_src->get_begin_ptr(), to_src - from_src);
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
};


LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  loff_t len)
{
  auto [begin, end] = bound(addr, addr+len);
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
  return seastar::do_with(
    bound(min, max),
    L_ADDR_NULL,
    [this, &cache, &t, min, max, len](auto &val, auto &ret) {
      auto &[i, e] = val;
      return crimson::do_until(
	[this, &cache, &t, &i, &e, &ret, len] {
	  if (i == e) {
	    return find_hole_ertr::make_ready_future<std::optional<laddr_t>>(
	      std::make_optional<laddr_t>(L_ADDR_NULL));
	  }
	  return get_lba_btree_extent(
	    cache,
	    t,
	    depth-1,
	    i->get_paddr()
	  ).safe_then([this, &cache, &t, &i, len](auto extent) mutable {
	    return extent->find_hole(
	      cache,
	      t,
	      i->get_lb(),
	      i->get_ub(),
	      len);
	  }).safe_then([&i, &ret](auto addr) mutable {
	    i++;
	    if (addr != L_ADDR_NULL) {
	      ret = addr;
	    }
	    return find_hole_ertr::make_ready_future<std::optional<laddr_t>>(
	      addr == L_ADDR_NULL ? std::nullopt :
	      std::make_optional<laddr_t>(addr));
	  });
	}).safe_then([&ret]() {
	  return ret;
	});
    });
}

std::tuple<LBANodeRef, LBANodeRef, laddr_t>
LBAInternalNode::make_split_children(Cache &cache, Transaction &t)
{
  auto left = cache.alloc_new_extent<LBAInternalNode>(
    t, LBA_BLOCK_SIZE);
  auto right = cache.alloc_new_extent<LBAInternalNode>(
    t, LBA_BLOCK_SIZE);
  auto piviter = get_split_pivot();

  left->copy_from_foreign(left->begin(), begin(), piviter);
  left->set_size(piviter - begin());
  left->set_pivot(get_pivot());

  right->copy_from_foreign(right->begin(), piviter, end());
  right->set_size(end() - piviter);
  right->set_pivot(piviter->get_lb());

  return std::make_tuple(left, right, piviter->get_lb());
}

LBAInternalNode::split_ret
LBAInternalNode::split_entry(
  Cache &c, Transaction &t, laddr_t addr, internal_iterator_t, LBANodeRef entry)
{
  ceph_assert(!at_max_capacity());
  auto [left, right, pivot] = entry->make_split_children(c, t);
  return split_ertr::make_ready_future<LBANodeRef>(
    pivot > addr ? left : right
  );
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

  std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_split_children(Cache &cache, Transaction &t) final;

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
