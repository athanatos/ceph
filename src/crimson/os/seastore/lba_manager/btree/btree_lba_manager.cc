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
BtreeLBAManager::get_mappings(
  laddr_t offset, loff_t length,
  Transaction &t)
{
  auto &lt = get_lba_trans(t);
  return cache.get_extent(
    t,
    lt.root.lba_root_addr,
    LBA_BLOCK_SIZE).safe_then([](auto) {
      return get_mapping_ret(
	get_mapping_ertr::ready_future_marker{},
	lba_pin_list_t());
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


