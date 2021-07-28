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
  } else {
    
  }

  return iterator_fut(
    interruptible::ready_future_marker{},
    *this);
}

LBABtree::iterator_fut LBABtree::iterator::prev(op_context_t c) const
{
  assert_valid();
  assert(0 == "TODO");
  // TODOSAM
  return iterator_fut(
    interruptible::ready_future_marker{},
    *this);
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
  return insert_ret(
    interruptible::ready_future_marker{},
    std::make_pair(iterator{}, true));
}

LBABtree::update_ret LBABtree::update(
  op_context_t c,
  iterator iter,
  lba_map_val_t val)
{
  LOG_PREFIX(LBATree::update);
  return update_ret(
    interruptible::ready_future_marker{},
    iterator{});
}

LBABtree::remove_ret LBABtree::remove(
  op_context_t c,
  iterator iter)
{
  LOG_PREFIX(LBATree::remove);
  return remove_ret(
    interruptible::ready_future_marker{},
    iterator{});
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

}
