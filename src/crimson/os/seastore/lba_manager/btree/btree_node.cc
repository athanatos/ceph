// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_node.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

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
  auto insertion_pt = get_insertion_point(laddr);
  return get_lba_btree_extent(
    cache,
    t,
    depth-1,
    insertion_pt->get_paddr()).safe_then(
      [this, insertion_pt, &cache, &t, laddr, val=std::move(val)](
	auto extent) mutable {
	return extent->at_max_capacity() ?
	  split_entry(cache, t, laddr, insertion_pt) :
	  insert_ertr::make_ready_future<LBANodeRef>(std::move(extent));
      }).safe_then([this, &cache, &t, laddr, val=std::move(val)](
		     auto extent) mutable {
	return extent->insert(cache, t, laddr, val);
      });
}

LBAInternalNode::remove_ret LBAInternalNode::remove(
  Cache &cache,
  Transaction &transaction,
  laddr_t)
{
  return remove_ertr::now();
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
  Cache &c, Transaction &t, laddr_t addr, internal_iterator_t&)
{
  return split_ertr::make_ready_future<LBANodeRef>();
}

LBAInternalNode::internal_iterator_t
LBAInternalNode::get_insertion_point(laddr_t laddr)
{
  return internal_iterator_t();
}

std::pair<LBAInternalNode::internal_iterator_t,
	  LBAInternalNode::internal_iterator_t>
LBAInternalNode::get_internal_entries(laddr_t addr, loff_t len)
{
  return std::make_pair(internal_iterator_t(), internal_iterator_t());
}

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
