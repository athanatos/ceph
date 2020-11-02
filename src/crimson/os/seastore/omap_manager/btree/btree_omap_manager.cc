// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "crimson/common/log.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::omap_manager {

BtreeOMapManager::BtreeOMapManager(
  TransactionManager &tm)
  : tm(tm) {}

BtreeOMapManager::initialize_omap_ret
BtreeOMapManager::initialize_omap(Transaction &t)
{

  logger().debug("{}", __func__);
  return tm.alloc_extent<OMapLeafNode>(t, L_ADDR_MIN, OMAP_BLOCK_SIZE)
    .safe_then([this](auto&& root_extent) {
      root_extent->set_size(0);
      omap_node_meta_t meta{1};
      root_extent->set_meta(meta);
      omap_root_t omap_root = omap_root_t(1, root_extent->get_laddr());
      return initialize_omap_ertr::make_ready_future<omap_root_t>(omap_root);
  });
}

BtreeOMapManager::get_root_ret
BtreeOMapManager::get_omap_root(omap_root_t &omap_root, Transaction &t)
{
  assert(omap_root.omap_root_laddr != L_ADDR_NULL);
  laddr_t laddr = omap_root.omap_root_laddr;
  return omap_load_extent(get_omap_context(omap_root, t), nullptr, laddr, omap_root.depth);
}

BtreeOMapManager::omap_get_value_ret
BtreeOMapManager::omap_get_value(omap_root_t &omap_root, Transaction &t,
                                 const std::string &key)
{
  logger().debug("{}: {}", __func__, key);
  return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t, &key](auto&& extent) {
    return extent->get_value(get_omap_context(omap_root, t), key);
  }).safe_then([](auto &&e) {
    logger().debug("{}: {} -> {}", __func__, e.first, e.second);
    return omap_get_value_ret(
        omap_get_value_ertr::ready_future_marker{},
        std::move(e));
  });

}

BtreeOMapManager::omap_set_key_ret
BtreeOMapManager::omap_set_key(omap_root_t &omap_root, Transaction &t,
                               std::string &key, std::string &value)
{
  logger().debug("{}: {} -> {}", __func__, key, value);
  return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t, &key, &value](auto &&root) {
    return insert_key(omap_root, t, root, key, value);
  }).safe_then([](auto ret) {
      logger().debug("{}:  {} -> {}", __func__,  ret.first, ret.second);
      return omap_set_key_ret(
        omap_set_key_ertr::ready_future_marker{},
        std::move(ret));
  });

}

BtreeOMapManager::omap_rm_key_ret
BtreeOMapManager::omap_rm_key(omap_root_t &omap_root, Transaction &t, const std::string &key)
{
  logger().debug("{}: {}", __func__, key);
  return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t, &key](auto extent) {
    return extent->rm_key(get_omap_context(omap_root, t), key);
  }).safe_then([](auto ret) {
    logger().debug("{}: {}", __func__, ret);
    return omap_rm_key_ret(
      omap_rm_key_ertr::ready_future_marker{},
      ret);
  });

}

BtreeOMapManager::omap_list_keys_ret
BtreeOMapManager::omap_list_keys(omap_root_t &omap_root, Transaction &t)
{
  logger().debug("{}", __func__);
  return seastar::do_with(std::vector<std::string>(), [this, &omap_root, &t]
    (auto &result) {
    return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t, &result](auto extent) {
      return extent->list_keys(get_omap_context(omap_root, t), result);
    }).safe_then([&result]() {
      return omap_list_keys_ret(
        omap_list_keys_ertr::ready_future_marker{},
        std::move(result));
    });
  });

}

BtreeOMapManager::omap_list_ret
BtreeOMapManager::omap_list(omap_root_t &omap_root, Transaction &t)
{
  logger().debug("{}", __func__);
  return seastar::do_with(std::vector<std::pair<std::string, std::string>>(),
    [this, &omap_root, &t] (auto &result) {
    return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t, &result](auto extent) {
      return extent->list(get_omap_context(omap_root, t), result);
    }).safe_then([&result]() {
      return omap_list_ret(
        omap_list_ertr::ready_future_marker{},
        std::move(result));
    });
  });
}

BtreeOMapManager::omap_clear_ret
BtreeOMapManager::omap_clear(omap_root_t &omap_root, Transaction &t)
{
  logger().debug("{}", __func__);
  return get_omap_root(omap_root, t).safe_then([this, &omap_root, &t](auto extent) {
    return extent->clear(get_omap_context(omap_root, t));
  }).safe_then([this, &omap_root, &t] {
    return tm.dec_ref(t, omap_root.omap_root_laddr).safe_then([&omap_root] (auto ret) {
      omap_root.state = omap_root_state_t::MUTATED;
      omap_root.depth = 0;
      omap_root.omap_root_laddr = L_ADDR_NULL;
      return omap_clear_ertr::now();
    });
  });
}

BtreeOMapManager::insert_key_ret
BtreeOMapManager::insert_key(omap_root_t &omap_root, Transaction &t,
                OMapNodeRef root, std::string &key, std::string &value)
{
  auto split = insert_key_ertr::make_ready_future<OMapNodeRef>(root);
  bool overflow;
  if (omap_root.depth >1)
    overflow = root->extent_is_overflow(key.size() + 1);
  else
    overflow = root->extent_is_overflow(key.size() + 1 + value.size() + 1);
  if (overflow) {
    logger().debug("{}::splitting root for Omaptree {}", __func__, *root);
    split =  root->omap_alloc_extent<OMapInnerNode>(get_omap_context(omap_root, t), nullptr, OMAP_BLOCK_SIZE)
      .safe_then([this, &omap_root, root, &t, &key](auto&& nroot) {
        omap_node_meta_t meta{root->get_node_meta().depth + 1};
        nroot->set_meta(meta);
        root->parent = nroot;
        omap_inner_key_t node_key;
        node_key.laddr = root->get_laddr();
        node_key.key_off = 1;
        node_key.key_len = 1;
        nroot->journal_inner_insert(nroot->iter_begin(), node_key, "\0", nullptr);
        omap_root.omap_root_laddr = nroot->get_laddr();
        omap_root.depth = root->get_node_meta().depth + 1;
        omap_root.state = omap_root_state_t::MUTATED;
        return nroot->split_entry(get_omap_context(omap_root, t), key, nroot->iter_begin(), root);
      });
  }
  return split.safe_then([this,  &omap_root, &t, &key, &value](OMapNodeRef node) {
    return node->insert(get_omap_context(omap_root, t), key, value);
  });
}

}
