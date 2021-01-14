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

std::ostream &CollectionNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size();

}

CollectionNode::list_ret
CollectionNode::list()
{
  logger().debug("CollectionNode:{}", __func__);
  std::vector<std::pair<coll_t, coll_info_t>> list_result;
  read_to_local();
  for (auto &&it : coll_kv) {
    coll_t cid;
    if (cid.parse(it.key)) {
      list_result.emplace_back(std::pair(cid, coll_info_t(it.val)));
    } else {
      logger().error("unrecognized collection");
    }
  }
  return list_ret(
    list_ertr::ready_future_marker{},
    std::move(list_result));
}

CollectionNode::create_ret
CollectionNode::create(coll_context_t cc, std::string key, unsigned bits)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->create(cc, key, bits);
  }
  read_to_local();
  bool ret = journal_insert(key, bits, maybe_get_delta_buffer());
  copy_to_node();
  return create_ertr::make_ready_future<bool>(ret);
}

CollectionNode::update_ret
CollectionNode::update(coll_context_t cc, std::string key, unsigned bits)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->update(cc, key, bits);
  }
  read_to_local();
  bool ret = journal_update(key, bits, maybe_get_delta_buffer());
  copy_to_node();
  return update_ertr::make_ready_future<bool>(ret);
}

CollectionNode::remove_ret
CollectionNode::remove(coll_context_t cc, std::string key)
{
  logger().debug("CollectionNode:{}", __func__);
  if (!is_pending()) {
    auto mut = cc.tm.get_mutable_extent(cc.t, this)->cast<CollectionNode>();
    return mut->remove(cc, key);
  }
  read_to_local();
  bool ret = journal_remove(key, maybe_get_delta_buffer());
  copy_to_node();
  return remove_ertr::make_ready_future<bool>(ret);
}

}
