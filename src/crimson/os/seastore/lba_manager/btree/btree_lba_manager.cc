// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::BtreeLBAManager(
  SegmentManager &segment_manager,
  Cache &cache)
  : segment_manager(segment_manager),
    cache(cache) {}


BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  laddr_t offset, loff_t length,
  Transaction &t)
{
  auto &lt = get_lba_trans(t);
  return get_lba_btree_extent(
    cache,
    t,
    lt.root.lba_root_addr).safe_then([this, &t, &lt, offset, length](auto extent) {
      return LBANode::get_node(lt.root.lba_depth, extent)->lookup_range(
	cache, t, offset, length);
    });
}


BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  lextent_list_t &&list,
  Transaction &t)
{
  auto l = std::make_unique<lextent_list_t>(std::move(list));
  auto retptr = std::make_unique<lba_pin_list_t>();
  auto &ret = *retptr;
  return crimson::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return get_mapping(p.first, p.second, t).safe_then(
	[this, &ret](auto res) {
	  ret.splice(ret.end(), res, res.begin(), res.end());
	});
    }).safe_then([this, l=std::move(l), retptr=std::move(retptr)]() mutable {
      return std::move(*retptr);
    });
}

BtreeLBAManager::alloc_extent_relative_ret
BtreeLBAManager::alloc_extent_relative(
  laddr_t hint,
  loff_t len,
  segment_off_t offset,
  Transaction &t)
{
  return alloc_extent_relative_ret(
    alloc_extent_relative_ertr::ready_future_marker{},
    LBAPinRef());
}

BtreeLBAManager::set_extent_ret
BtreeLBAManager::set_extent(
  laddr_t off, loff_t len, paddr_t addr,
  Transaction &t)
{
  return set_extent_ret(
    set_extent_ertr::ready_future_marker{},
    LBAPinRef());
}

BtreeLBAManager::set_extent_relative_ret
BtreeLBAManager::set_extent_relative(
  laddr_t off, loff_t len, segment_off_t record_offset,
  Transaction &t)
{
  return set_extent_relative_ret(
    set_extent_relative_ertr::ready_future_marker{},
    LBAPinRef());
}

bool BtreeLBAManager::decref_extent(LBAPinRef &ref, Transaction &t)
{
  return true;
}

void BtreeLBAManager::incref_extent(LBAPinRef &ref, Transaction &t)
{
}

BtreeLBAManager::submit_lba_transaction_ret
BtreeLBAManager::submit_lba_transaction(
  Transaction &t)
{
  return submit_lba_transaction_ertr::now();
}

}


