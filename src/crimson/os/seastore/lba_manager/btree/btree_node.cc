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

std::unique_ptr<LBANode> LBANode::get_node(
  depth_t depth,
  CachedExtentRef extent)
{
  return std::unique_ptr<LBANode>(
    depth > 0 ?
    static_cast<LBANode*>(new LBAInternalNode(depth, extent)) :
    static_cast<LBANode*>(new LBALeafNode(depth, extent)));
}



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
      return cache.get_extent(
	t, val.get_paddr(), LBA_BLOCK_SIZE).safe_then(
	  [this, &cache, &t, &result, addr, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return LBANode::get_node(depth - 1, extent)->lookup_range(
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

std::pair<LBAInternalNode::internal_iterator_t,
	  LBAInternalNode::internal_iterator_t>
LBAInternalNode::get_internal_entries(laddr_t addr, loff_t len)
{
  return std::make_pair(internal_iterator_t(), internal_iterator_t());
}

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
	extent,
	(*i).get_paddr(),
	(*i).get_laddr(),
	(*i).get_length()));
  }
  return lookup_range_ertr::make_ready_future<lba_pin_list_t>(
    std::move(ret));
}

std::pair<LBALeafNode::internal_iterator_t, LBALeafNode::internal_iterator_t>
LBALeafNode::get_leaf_entries(laddr_t addr, loff_t len)
{
  return std::make_pair(internal_iterator_t(), internal_iterator_t());
}

}
