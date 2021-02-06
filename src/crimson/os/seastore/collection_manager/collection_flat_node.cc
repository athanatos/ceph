// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"
#include "osd/osd_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::collection_manager {

void delta_t::replay(coll_map_t &l) const
{
  switch (op) {
  case op_t::INSERT: {
    l.insert(coll, bits);
    break;
  }
  case op_t::UPDATE: {
    l.update(coll, bits);
    break;
  }
  case op_t::REMOVE: {
    l.erase(coll);
    break;
  }
  case op_t::INVALID: {
    assert(0 == "impossible");
    break;
  }
  __builtin_unreachable();
  }
}


std::ostream &CollectionNode::print_detail_l(std::ostream &out) const
{
  return out;
}

CollectionNode::list_ret
CollectionNode::list()
{
  logger().debug("CollectionNode:{}, {}", __func__, *this);
  std::vector<std::pair<coll_t, coll_info_t>> list_result;
  for (auto &&it : decoded) {
    list_result.emplace_back(
      std::make_pair(
	static_cast<coll_t>(it.first),
	coll_info_t{ it.second }
      ));
  }
  return list_ret(
    list_ertr::ready_future_marker{},
    std::move(list_result));
}

CollectionNode::create_ret
CollectionNode::create(coll_context_t cc, coll_t coll, unsigned bits)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->create(cc, coll, bits);
  }
  // TODO check for existence
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->insert(coll, bits);
  }
  logger().debug("CollectionNode::create {} {} {}", coll, bits, *this);
  decoded.insert(coll, bits);
  copy_to_node();
  return seastar::now();
}

CollectionNode::update_ret
CollectionNode::update(coll_context_t cc, coll_t coll, unsigned bits)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->update(cc, coll, bits);
  }
  // TODO check for existence
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->insert(coll, bits);
  }
  decoded.update(coll, bits);
  copy_to_node();
  return seastar::now();
}

CollectionNode::remove_ret
CollectionNode::remove(coll_context_t cc, coll_t coll)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->remove(cc, coll);
  }
  // TODO check for existence
  if (auto buffer = maybe_get_delta_buffer(); buffer) {
    buffer->remove(coll);
  }
  decoded.remove(coll);
  copy_to_node();
  return seastar::now();
}

}
