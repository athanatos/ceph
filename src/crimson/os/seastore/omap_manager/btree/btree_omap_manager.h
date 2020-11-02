// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore::omap_manager {

class BtreeOMapManager : public OMapManager {
  TransactionManager &tm;

  omap_context_t get_omap_context(omap_root_t &omap_root, Transaction &t) {
    return omap_context_t{omap_root, tm, t};
  }

  /* get_omap_root
   *
   * load omap tree root node
   */
  using get_root_ertr = TransactionManager::read_extent_ertr;
  using get_root_ret = get_root_ertr::future<OMapNodeRef>;
  get_root_ret get_omap_root(omap_root_t &omap_root, Transaction &t);

  using insert_key_ertr = TransactionManager::read_extent_ertr;
  using insert_key_ret = insert_key_ertr::future<std::pair<std::string, std::string>>;
  insert_key_ret insert_key(omap_root_t &omap_root, Transaction &t,
                            OMapNodeRef extent, std::string &key,
                            std::string &val);

public:
  explicit BtreeOMapManager(TransactionManager &tm);

  initialize_omap_ret initialize_omap(Transaction &t) final;

  omap_get_value_ret omap_get_value(omap_root_t &omap_root, Transaction &t,
                                    const std::string &key) final;

  omap_set_key_ret omap_set_key(omap_root_t &omap_root, Transaction &t,
                                std::string &key, std::string &value) final;

  omap_rm_key_ret omap_rm_key(omap_root_t &omap_root, Transaction &t,
                              const std::string &key) final;

  omap_list_keys_ret omap_list_keys(omap_root_t &omap_root, Transaction &t) final;

  omap_list_ret omap_list(omap_root_t &omap_root, Transaction &t) final;

  omap_clear_ret omap_clear(omap_root_t &omap_root, Transaction &t) final;

};
using BtreeOMapManagerRef = std::unique_ptr<BtreeOMapManager>;

}
