// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string.h>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/string_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"

namespace crimson::os::seastore::omap_manager {

/**
 * OMapInnerNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * omap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta    :    keys    :  values  :
 */

struct OMapInnerNode
  : OMapNode,
    StringKVInnerNodeLayout<
    omap_node_meta_t, omap_node_meta_le_t> {
  using OMapInnerNodeRef = TCachedExtentRef<OMapInnerNode>;
  using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapInnerNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVInnerNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::OMAP_INNER;

  omap_node_meta_t get_node_meta() const final {return get_meta();}
  bool extent_is_overflow(size_t size) {return is_overflow(size);}
  bool extent_under_median() {return under_median();}
  uint32_t get_node_size() {return get_size();}

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapInnerNode(*this));
  }

  delta_inner_buffer_t delta_buffer;
  delta_inner_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(omap_context_t oc, const std::string &key) final;

  insert_ret insert(omap_context_t oc, std::string &key, std::string &value) final;

  rm_key_ret rm_key(omap_context_t oc, const std::string &key) final;

  list_keys_ret list_keys(omap_context_t oc, std::vector<std::string> &result) final;

  list_ret list(omap_context_t oc, std::vector<std::pair<std::string, std::string>> &result) final;

  clear_ret clear(omap_context_t oc) final;

  split_children_ret make_split_children(omap_context_t oc, OMapNodeRef pnode) final;

  full_merge_ret make_full_merge(omap_context_t oc, OMapNodeRef right, OMapNodeRef pnode) final;

  make_balanced_ret
    make_balanced(omap_context_t oc, OMapNodeRef right, bool prefer_left, OMapNodeRef pnode) final;

  std::ostream &print_detail_l(std::ostream &out) const final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    delta_buffer.encode(bl);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_inner_buffer_t buffer;
    buffer.decode(bl);
    buffer.replay(*this);
  }

  using split_entry_ertr = TransactionManager::read_extent_ertr;
  using split_entry_ret = split_entry_ertr::future<OMapNodeRef>;
  split_entry_ret split_entry(omap_context_t oc, std::string &key,
                              internal_iterator_t, OMapNodeRef entry);

  using make_split_entry_ertr = TransactionManager::read_extent_ertr;
  using make_split_entry_ret = make_split_entry_ertr::future
        <std::tuple<OMapNodeRef, OMapNodeRef, std::string>>;
  make_split_entry_ret make_split_entry(omap_context_t oc, std::string key,
                              internal_iterator_t, OMapNodeRef entry);

  using checking_parent_ertr = TransactionManager::read_extent_ertr;
  using checking_parent_ret = checking_parent_ertr::future
        <std::pair<OMapInnerNodeRef, internal_iterator_t>>;
  checking_parent_ret checking_parent(omap_context_t oc, std::string key,
                                     internal_iterator_t, OMapInnerNodeRef entry);

  using merge_entry_ertr = TransactionManager::read_extent_ertr;
  using merge_entry_ret = merge_entry_ertr::future<OMapNodeRef>;
  merge_entry_ret merge_entry(omap_context_t oc, const std::string &key,
                              internal_iterator_t iter, OMapNodeRef entry);

  internal_iterator_t get_containing_child(const std::string &key);

};
using OMapInnerNodeRef = OMapInnerNode::OMapInnerNodeRef;
/**
 * OMapLeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * OMap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta   :   keys   :  values  :
 */

struct OMapLeafNode
  : OMapNode,
    StringKVLeafNodeLayout<
      omap_node_meta_t, omap_node_meta_le_t> {

  using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapLeafNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVLeafNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::OMAP_LEAF;

  omap_node_meta_t get_node_meta() const final { return get_meta(); }
  bool extent_is_overflow(size_t size) {return is_overflow(size);}
  bool extent_under_median() {return under_median();}
  uint32_t get_node_size() {return get_size();}

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapLeafNode(*this));
  }

  delta_leaf_buffer_t delta_buffer;
  delta_leaf_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(omap_context_t oc, const std::string &key) final;

  insert_ret insert(omap_context_t oc, std::string &key, std::string &value) final;

  rm_key_ret rm_key(omap_context_t oc, const std::string &key) final;

  list_keys_ret list_keys(omap_context_t oc, std::vector<std::string> &result) final;

  list_ret list(omap_context_t oc, std::vector<std::pair<std::string, std::string>> &result) final;

  clear_ret clear(omap_context_t oc) final;

  split_children_ret make_split_children(omap_context_t oc, OMapNodeRef pnode) final;

  full_merge_ret make_full_merge(omap_context_t oc, OMapNodeRef right, OMapNodeRef pnode) final;

  make_balanced_ret make_balanced(omap_context_t oc, OMapNodeRef _right, bool prefer_left,
                                  OMapNodeRef pnode) final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    delta_buffer.encode(bl);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &_bl) final {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    delta_leaf_buffer_t buffer;
    buffer.decode(bl);
    buffer.replay(*this);
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(std::string &key);

};
using OMapLeafNodeRef = TCachedExtentRef<OMapLeafNode>;

std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs);
std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs);
}
