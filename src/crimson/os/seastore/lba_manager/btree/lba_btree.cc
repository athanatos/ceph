// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"
#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

LBABtree::mkfs_ret LBABtree::mkfs(op_context_t c)
{
  auto root_leaf = c.cache.alloc_new_extent<LBALeafNode>(
    c.trans,
    LBA_BLOCK_SIZE);
  root_leaf->set_size(0);
  lba_node_meta_t meta{0, L_ADDR_MAX, 1};
  root_leaf->set_meta(meta);
  root_leaf->pin.set_range(meta);
  return lba_root_t{root_leaf->get_paddr(), 1u};
}

LBABtree::iterator_fut LBABtree::iterator::next(op_context_t c) const
{
  assert_valid();
  assert(!is_end());

  if ((leaf.pos + 1) < leaf.node->get_size()) {
    auto ret = *this;
    ret.leaf.pos++;
    return iterator_fut(
      interruptible::ready_future_marker{},
      ret);
  }

  depth_t depth_with_space = 2;
  for (; depth_with_space <= get_depth(); ++depth_with_space) {
    if ((get_internal(depth_with_space).pos + 1) <
	get_internal(depth_with_space).node->get_size()) {
      break;
    }
  }

  if (depth_with_space < get_depth()) {
    return seastar::do_with(
      *this,
      [](const LBAInternalNode &internal) { return internal.begin(); },
      [](const LBALeafNode &leaf) { return leaf.begin(); },
      [c, depth_with_space](auto &ret, auto &li, auto &ll) {
	for (depth_t depth = 2; depth < depth_with_space; ++depth) {
	  ret.get_internal(depth).reset();
	}
	ret.leaf.reset();
	ret.get_internal(depth_with_space).pos++;
	return lookup_depth_range(
	  c, ret, depth_with_space - 1, 0, li, ll
	).si_then([&ret] {
	  return std::move(ret);
	});
      });
  } else {
    // end
    auto ret = *this;
    ret.leaf.pos = MAX;
    return iterator_fut(
      interruptible::ready_future_marker{},
      ret);
  }
}

LBABtree::iterator_fut LBABtree::iterator::prev(op_context_t c) const
{
  assert_valid();
  assert(!is_begin());

  if (leaf.pos > 0) {
    auto ret = *this;
    ret.leaf.pos--;
    return iterator_fut(
      interruptible::ready_future_marker{},
      ret);
  }

  depth_t depth_with_space = 2;
  for (; depth_with_space <= get_depth(); ++depth_with_space) {
    if (get_internal(depth_with_space).pos > 0) {
      break;
    }
  }

  assert(depth_with_space < get_depth()); // must not be begin()
  return seastar::do_with(
    *this,
    [](const LBAInternalNode &internal) { return --internal.end(); },
    [](const LBALeafNode &leaf) { return --leaf.end(); },
    [c, depth_with_space](auto &ret, auto &li, auto &ll) {
      for (depth_t depth = 2; depth < depth_with_space; ++depth) {
	ret.get_internal(depth).reset();
      }
      ret.leaf.reset();
      ret.get_internal(depth_with_space).pos--;
      return lookup_depth_range(
	c, ret, depth_with_space - 1, 0, li, ll
      ).si_then([&ret] {
	return std::move(ret);
      });
    });
}

LBABtree::iterator_fut LBABtree::lower_bound(
  op_context_t c,
  laddr_t addr) const
{
  LOG_PREFIX(LBATree::lower_bound);
  return lookup(
    c,
    [addr](const LBAInternalNode &internal) {
      assert(internal.get_size() > 0);
      auto iter = internal.upper_bound(addr);
      assert(iter != internal.begin());
      --iter;
      return iter;
    },
    [addr](const LBALeafNode &leaf) {
      assert(leaf.get_size() > 0);
      return leaf.lower_bound(addr);
    }).si_then([](auto &&ret) {
      ret.assert_valid();
      return std::move(ret);
    });
}

LBABtree::insert_ret LBABtree::insert(
  op_context_t c,
  iterator iter,
  laddr_t laddr,
  lba_map_val_t val)
{
  LOG_PREFIX(LBATree::insert);
  return seastar::do_with(
    iter,
    [this, c, laddr, val](auto &ret) {
      return find_insertion(
	c, laddr, ret
      ).si_then([this, c, laddr, val, &ret] {
	if (ret.leaf.pos != iterator::MAX && ret.get_key() == laddr) {
	  return insert_ret(
	    interruptible::ready_future_marker{},
	    std::make_pair(ret, false));
	} else {
	  return handle_split(
	    c, ret
	  ).si_then([c, laddr, val, &ret] {
	    if (!ret.leaf.node->is_pending()) {
	      CachedExtentRef mut = c.cache.duplicate_for_write(
		c.trans, ret.leaf.node
	      );
	      ret.leaf.node = mut->cast<LBALeafNode>();
	    }
	    // ret.leaf.pos = ret.leaf.node->insert(c, laddr, val);
	    return insert_ret(
	      interruptible::ready_future_marker{},
	      std::make_pair(ret, true));
	  });
	}
      });
    });
}

LBABtree::update_ret LBABtree::update(
  op_context_t c,
  iterator ret,
  lba_map_val_t val)
{
  LOG_PREFIX(LBATree::update);
  if (!ret.leaf.node->is_pending()) {
    CachedExtentRef mut = c.cache.duplicate_for_write(
      c.trans, ret.leaf.node
    );
    ret.leaf.node = mut->cast<LBALeafNode>();
  }
  // ret.leaf.node->update(
  //   ret.leaf.node->iter_idx(ret.leaf.pos),
  //   val);
  return update_ret(
    interruptible::ready_future_marker{},
    ret);
}

LBABtree::remove_ret LBABtree::remove(
  op_context_t c,
  iterator iter)
{
  LOG_PREFIX(LBATree::remove);
  assert(!iter.is_end());
  return seastar::do_with(
    iter,
    [this, c](auto &ret) {
      return handle_merge(
	c, ret
      ).si_then([c, &ret] {
	if (!ret.leaf.node->is_pending()) {
	  CachedExtentRef mut = c.cache.duplicate_for_write(
	    c.trans, ret.leaf.node
	  );
	  ret.leaf.node = mut->cast<LBALeafNode>();
	}
	// ret.leaf.pos = ret.leaf.node->remove(
	//   c, ret.leaf.node->iter_idx(ret.leaf.pos)
	// );
	return ret.next(c);
      });
    });
}

LBABtree::get_internal_node_ret LBABtree::get_internal_node(
  op_context_t c,
  depth_t depth,
  paddr_t offset)
{
  LOG_PREFIX(LBATree::get_internal_node);
  DEBUGT(
    "reading internal at offset {}, depth {}",
    c.trans,
    offset,
    depth);
    return c.cache.get_extent<LBAInternalNode>(
      c.trans,
      offset,
      LBA_BLOCK_SIZE
    ).si_then([FNAME, c, offset](LBAInternalNodeRef ret) {
      DEBUGT(
	"read internal at offset {} {}",
	c.trans,
	offset,
	*ret);
      auto meta = ret->get_meta();
      if (ret->get_size()) {
	ceph_assert(meta.begin <= ret->begin()->get_key());
	ceph_assert(meta.end > (ret->end() - 1)->get_key());
      }
      if (!ret->is_pending() && !ret->pin.is_linked()) {
	ret->pin.set_range(meta);
	c.pins.add_pin(ret->pin);
      }
      return get_internal_node_ret(
	interruptible::ready_future_marker{},
	ret);
    });
}

LBABtree::get_leaf_node_ret LBABtree::get_leaf_node(
  op_context_t c,
  paddr_t offset)
{
  LOG_PREFIX(LBATree::get_leaf_node);
  DEBUGT(
    "reading leaf at offset {}",
    c.trans,
    offset);
  return c.cache.get_extent<LBALeafNode>(
    c.trans,
    offset,
    LBA_BLOCK_SIZE
  ).si_then([FNAME, c, offset](LBALeafNodeRef ret) {
    DEBUGT(
      "read leaf at offset {} {}",
      c.trans,
      offset,
      *ret);
    auto meta = ret->get_meta();
    if (ret->get_size()) {
      ceph_assert(meta.begin <= ret->begin()->get_key());
      ceph_assert(meta.end > (ret->end() - 1)->get_key());
    }
    if (!ret->is_pending() && !ret->pin.is_linked()) {
      ret->pin.set_range(meta);
      c.pins.add_pin(ret->pin);
    }
    return get_leaf_node_ret(
      interruptible::ready_future_marker{},
      ret);
  });
}

LBABtree::find_insertion_ret LBABtree::find_insertion(
  op_context_t c,
  laddr_t laddr,
  iterator &iter)
{
  return seastar::now();
}

LBABtree::handle_split_ret LBABtree::handle_split(
  op_context_t c,
  iterator &iter)
{
  return seastar::now();
}

LBABtree::handle_merge_ret LBABtree::handle_merge(
  op_context_t c,
  iterator &iter)
{
  return seastar::now();
}

}
