// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_node.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {


LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  loff_t len)
{
  auto [begin, end] = get_internal_entries(addr, len);
  auto result = std::make_unique<lba_pin_list_t>();
  return seastar::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &cache, &t, addr, len](const auto &val) {
      return cache.lookup(t, val.get_paddr, val.get_length).safe_then(
	[&cache, t, addr, len](auto extent) {
	  return lookup_range_ertr::make_ready_future<lba_pin_list_t>();
	});
    });
}

LBALeafNode::lookup_range_ret LBALeafNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  loff_t len)
{
}

LBALeafNode::lookup_entries_ret LBALeafNode::get_leaf_entries(
  laddr_t addr, loff_t len)
{
}
