// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"


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

BtreeLBAManager::mkfs_ret BtreeLBAManager::mkfs(
  Transaction &t)
{
  logger().debug("BtreeLBAManager::mkfs");
  return cache.get_root(t).safe_then([this, &t](auto root) {
    auto root_leaf = cache.alloc_new_extent<LBALeafNode>(
      t,
      LBA_BLOCK_SIZE);
    root->set_lba_root(
      btree_lba_root_t{
	0,
	0,
	root_leaf->get_paddr(),
	paddr_t{}});
    return mkfs_ertr::now();
  });
}

BtreeLBAManager::get_root_ret
BtreeLBAManager::get_root(Transaction &t)
{
  return cache.get_root(t).safe_then([this, &t](auto root) {
    return get_lba_btree_extent(
      cache,
      t,
      root->get_lba_root().lba_depth,
      root->get_lba_root().lba_root_addr);
  });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t offset, loff_t length)
{
  return get_root(
    t).safe_then([this, &t, offset, length](auto extent) {
      return extent->lookup_range(
	cache, t, offset, length);
    });
}


BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_list_t &&list)
{
  auto l = std::make_unique<laddr_list_t>(std::move(list));
  auto retptr = std::make_unique<lba_pin_list_t>();
  auto &ret = *retptr;
  return crimson::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return get_mapping(t, p.first, p.second).safe_then(
	[this, &ret](auto res) {
	  ret.splice(ret.end(), res, res.begin(), res.end());
	});
    }).safe_then([this, l=std::move(l), retptr=std::move(retptr)]() mutable {
      return std::move(*retptr);
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  loff_t len,
  paddr_t addr)
{
  return get_root(
    t).safe_then([this, &t, hint, len, addr](auto extent) {
      return extent->find_hole(
	cache,
	t,
	hint,
	L_ADDR_MAX,
	len).safe_then([extent = std::move(extent)](auto ret) {
	  return std::make_pair(ret, std::move(extent));
	});
    }).safe_then([this, &t, hint, len, addr](auto p) {
      auto &[ret, extent] = p;
      ceph_assert(ret != L_ADDR_MAX);
      return insert_mapping(
	t,
	extent,
	ret,
	{ len, addr }
      ).safe_then([extent](auto ret) {
	return alloc_extent_ret(
	  alloc_extent_ertr::ready_future_marker{},
	  LBAPinRef(ret.release()));
      });
    });
}

BtreeLBAManager::set_extent_ret
BtreeLBAManager::set_extent(
  Transaction &t,
  laddr_t off, loff_t len, paddr_t addr)
{
  return get_root(
    t).safe_then([this, &t, off, len, addr](auto root) {
      return insert_mapping(
	t,
	root,
	off,
	{ len, addr });
    }).safe_then([](auto ret) {
      return set_extent_ret(
	set_extent_ertr::ready_future_marker{},
	LBAPinRef(ret.release()));
    });
}

BtreeLBAManager::decref_extent_ret
BtreeLBAManager::decref_extent(
  Transaction &t,
  LBAPin &ref)
{
  return ref_ertr::make_ready_future<bool>(true);
}

BtreeLBAManager::incref_extent_ret
BtreeLBAManager::incref_extent(
  Transaction &t,
  LBAPin &ref)
{
  return ref_ertr::now();
}

BtreeLBAManager::submit_lba_transaction_ret
BtreeLBAManager::submit_lba_transaction(
  Transaction &t)
{
  return submit_lba_transaction_ertr::now();
}

BtreeLBAManager::insert_mapping_ret BtreeLBAManager::insert_mapping(
  Transaction &t,
  LBANodeRef root,
  laddr_t laddr,
  lba_map_val_t val)
{
  return root->insert(
    cache,
    t,
    laddr,
    val);
}

}
