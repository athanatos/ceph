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

LBABtree::iterator_fut LBABtree::lower_bound(
  op_context_t c,
  laddr_t addr) const
{
  LOG_PREFIX(lower_bound);
  return iterator_fut(
    interruptible::ready_future_marker{},
    iterator{});
}

LBABtree::iterator_fut LBABtree::upper_bound_right(
  op_context_t c,
  laddr_t addr) const
{
  LOG_PREFIX(upper_bound_right);
  return iterator_fut(
    interruptible::ready_future_marker{},
    iterator{});
}

LBABtree::insert_ret LBABtree::insert(
  op_context_t c,
  iterator iter,
  laddr_t laddr,
  lba_map_val_t val)
{
  LOG_PREFIX(insert);
  return insert_ret(
    interruptible::ready_future_marker{},
    std::make_pair(iterator{}, true));
}

LBABtree::update_ret LBABtree::update(
  op_context_t c,
  iterator iter,
  lba_map_val_t val)
{
  LOG_PREFIX(update);
  return update_ret(
    interruptible::ready_future_marker{},
    iterator{});
}

LBABtree::remove_ret LBABtree::remove(
  op_context_t c,
  iterator iter)
{
  LOG_PREFIX(remove);
  return remove_ret(
    interruptible::ready_future_marker{},
    iterator{});
}

}
