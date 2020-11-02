// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"

namespace crimson::os::seastore::omap_manager{

struct omap_context_t {
  omap_root_t &omap_root;
  TransactionManager &tm;
  Transaction &t;
};

struct OMapNode : LogicalCachedExtent {
  using OMapNodeRef = TCachedExtentRef<OMapNode>;
  OMapNodeRef parent = nullptr;

  OMapNode(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  OMapNode(const OMapNode &other)
  : LogicalCachedExtent(other) {}

  using get_value_ertr = OMapManager::omap_get_value_ertr;
  using get_value_ret = OMapManager::omap_get_value_ret;
  virtual get_value_ret get_value(omap_context_t oc, const std::string &key) = 0;

  using insert_ertr = OMapManager::omap_set_key_ertr;
  using insert_ret = OMapManager::omap_set_key_ret;
  virtual insert_ret insert(omap_context_t oc, std::string &key, std::string &value) = 0;

  using rm_key_ertr = OMapManager::omap_rm_key_ertr;
  using rm_key_ret = OMapManager::omap_rm_key_ret;
  virtual rm_key_ret rm_key(omap_context_t oc, const std::string &key) = 0;

  using list_keys_ertr = OMapManager::omap_list_keys_ertr;
  using list_keys_ret = list_keys_ertr::future<>;
  virtual list_keys_ret list_keys(omap_context_t oc, std::vector<std::string> &result) = 0;

  using list_ertr = OMapManager::omap_list_ertr;
  using list_ret = list_ertr::future<>;
  virtual list_ret list(omap_context_t oc,
                std::vector<std::pair<std::string, std::string>> &result) = 0;

  using clear_ertr = OMapManager::omap_clear_ertr;
  using clear_ret = clear_ertr::future<>;
  virtual clear_ret clear(omap_context_t oc) = 0;

  using split_children_ertr = TransactionManager::alloc_extent_ertr;
  using split_children_ret = split_children_ertr::future
          <std::tuple<OMapNodeRef, OMapNodeRef, std::string>>;
  virtual split_children_ret make_split_children(omap_context_t oc, OMapNodeRef pnode) = 0;

  using full_merge_ertr = TransactionManager::alloc_extent_ertr;
  using full_merge_ret = full_merge_ertr::future<OMapNodeRef>;
  virtual full_merge_ret make_full_merge(omap_context_t oc, OMapNodeRef right, OMapNodeRef pnode) = 0;

  using make_balanced_ertr = TransactionManager::alloc_extent_ertr;
  using make_balanced_ret = make_balanced_ertr::future
          <std::tuple<OMapNodeRef, OMapNodeRef, std::string>>;
  virtual make_balanced_ret make_balanced(omap_context_t oc, OMapNodeRef _right, bool prefer_left, OMapNodeRef pnode) = 0;

  virtual omap_node_meta_t get_node_meta() const = 0;
  virtual bool extent_is_overflow(size_t size) = 0;
  virtual bool extent_under_median() = 0;
  virtual uint32_t get_node_size() = 0;

  virtual ~OMapNode() = default;

  using alloc_ertr = TransactionManager::alloc_extent_ertr;
  template<class T>
  alloc_ertr::future<TCachedExtentRef<T>>
  omap_alloc_extent(omap_context_t oc, OMapNodeRef pnode, extent_len_t len) {
    return oc.tm.alloc_extent<T>(oc.t, L_ADDR_MIN, len).safe_then(
      [pnode](auto&& extent) {
      extent->parent = pnode;
      return alloc_ertr::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
    });
  }

  template<class T>
  alloc_ertr::future<std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>
  omap_alloc_2extents(omap_context_t oc, OMapNodeRef pnode, extent_len_t len) {
    return seastar::do_with(std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>(),
      [oc, pnode, len] (auto &extents) {
      return crimson::do_for_each(
                      boost::make_counting_iterator(0),
                      boost::make_counting_iterator(2),
        [oc, pnode, len, &extents] (auto i) {
        return oc.tm.alloc_extent<T>(oc.t, L_ADDR_MIN, len).safe_then(
          [i, pnode, &extents](auto &&node) {
          if (i == 0) {
            node->parent = pnode;
            extents.first = node;
          }
          if (i == 1) {
            node->parent = pnode;
            extents.second = node;
          }
        });
      }).safe_then([&extents] {
        return alloc_ertr::make_ready_future
          <std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>(std::move(extents));
      });
    });
  }

  using retire_ertr = crimson::errorator<
  crimson::ct_error::enoent,
  crimson::ct_error::input_output_error>;
  using retire_ret = retire_ertr::future<std::list<unsigned>>;
  retire_ret
  omap_retire_node(omap_context_t oc, std::list<laddr_t> dec_laddrs) {
    return seastar::do_with(std::move(dec_laddrs), std::list<unsigned>(),
      [oc] (auto &&dec_laddrs, auto &refcnt) {
      return crimson::do_for_each(dec_laddrs.begin(), dec_laddrs.end(),
        [oc, &refcnt] (auto &laddr) {
        return oc.tm.dec_ref(oc.t, laddr).safe_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
        });
      }).safe_then([&refcnt] {
        return retire_ertr::make_ready_future<std::list<unsigned>>(std::move(refcnt));
      });
    });
  }

};

using OMapNodeRef = OMapNode::OMapNodeRef;

TransactionManager::read_extent_ertr::future<OMapNodeRef>
omap_load_extent(omap_context_t oc, OMapNodeRef pnode, laddr_t laddr, depth_t depth);

}
