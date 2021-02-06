// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "include/stringify.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::collection_manager {

FlatCollectionManager::FlatCollectionManager(
  TransactionManager &tm)
  : tm(tm) {}

FlatCollectionManager::mkfs_ret
FlatCollectionManager::mkfs(Transaction &t, unsigned block_size)
{

  logger().debug("FlatCollectionManager: {}", __func__);
  return tm.alloc_extent<CollectionNode>(t, L_ADDR_MIN, block_size)
    .safe_then([this, block_size](auto&& root_extent) {
      coll_root_t coll_root = coll_root_t(root_extent->get_laddr());
      coll_block_size = block_size;
      return mkfs_ertr::make_ready_future<coll_root_t>(coll_root);
  }).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in FlatCollectionManager::mkfs"
  });
}

FlatCollectionManager::get_root_ret
FlatCollectionManager::get_coll_root(const coll_root_t &coll_root, Transaction &t)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  assert(coll_root.get_location() != L_ADDR_NULL);
  laddr_t laddr = coll_root.get_location();
  auto cc = get_coll_context(t);
  return cc.tm.read_extents<CollectionNode>(cc.t, laddr, coll_block_size).safe_then(
    [](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      return get_root_ertr::make_ready_future<CollectionNodeRef>(std::move(e));
  }).handle_error(
    get_root_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in FlatCollectionManager::get_coll_root"
  });
}

FlatCollectionManager::create_ret
FlatCollectionManager::create(coll_root_t &coll_root, Transaction &t,
                              coll_t cid, coll_info_t info)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t
  ).safe_then([this, &coll_root, &t, cid, info] (auto &&extent) {
    return extent->create(get_coll_context(t), cid, info.split_bits);
  });
}

FlatCollectionManager::list_ret
FlatCollectionManager::list(const coll_root_t &coll_root, Transaction &t)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t
  ).safe_then([this, &t] (auto extent) {
    return extent->list();
  });
}

FlatCollectionManager::update_ret
FlatCollectionManager::update(coll_root_t &coll_root, Transaction &t,
                              coll_t cid, coll_info_t info)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t
  ).safe_then([this, &coll_root, &t, cid, info] (auto extent) {
    return extent->update(get_coll_context(t), cid, info.split_bits);
  });
}

FlatCollectionManager::remove_ret
FlatCollectionManager::remove(const coll_root_t &coll_root, Transaction &t,
                              coll_t cid )
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t).safe_then([this, &t, cid] (auto extent) {
    return extent->remove(get_coll_context(t), cid);
  });
}

}
