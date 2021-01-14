// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager.h"
#include "crimson/os/seastore/collection_manager/flat_node_layout.h"

namespace crimson::os::seastore::collection_manager{
struct coll_context_t {
  TransactionManager &tm;
  Transaction &t;
};

struct CollectionNode
  : LogicalCachedExtent,
    FlatNodeLayout {
  using CollectionNodeRef = TCachedExtentRef<CollectionNode>;

  template <typename... T>
  CollectionNode(T&&... t)
  : LogicalCachedExtent(std::forward<T>(t)...),
    FlatNodeLayout() {}

  static constexpr extent_types_t type = extent_types_t::COLL_BLOCK;

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new CollectionNode(*this));
  }
  delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  using list_ertr = CollectionManager::list_ertr;
  using list_ret = CollectionManager::list_ret;
  list_ret list();

  using create_ertr = CollectionManager::create_ertr;
  using create_ret = CollectionManager::create_ret;
  create_ret create(coll_context_t cc, std::string key, unsigned bits);

  using remove_ertr = CollectionManager::remove_ertr;
  using remove_ret = CollectionManager::remove_ret;
  remove_ret remove(coll_context_t cc, std::string key);

  using update_ertr = CollectionManager::update_ertr;
  using update_ret = CollectionManager::update_ret;
  update_ret update(coll_context_t cc, std::string key, unsigned bits);

  void read_to_local() {
    ceph::buffer::ptr bptr(get_length());
    memcpy(bptr.c_str(), get_bptr().c_str(), get_length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    auto p = bl.cbegin();
    unsigned num;
    ceph::decode(num, p);
    coll_kv.resize(num);
    for (auto &&i : coll_kv) {
      ceph::decode(i.key, p);
      ceph::decode(i.val, p);
    }

  }

  void copy_to_node() {
    bufferlist bl;
    unsigned num = coll_kv.size();
    ceph::encode(num, bl);
    for (auto &&i : coll_kv) {
      ceph::encode(i.key, bl);
      ceph::encode(i.val, bl);
    }
    bl.rebuild();
    memset(get_bptr().c_str(),0, get_length());
    memcpy(get_bptr().c_str(), bl.front().c_str(), bl.front().length());
  }

  void copy_from_other(CollectionNodeRef other) {
    memcpy(get_bptr().c_str(), other->get_bptr().c_str(), other->get_length());
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    delta_buffer.encode(bl);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_buffer_t buffer;
    buffer.decode(bl);
    read_to_local();
    buffer.replay(*this);
    copy_to_node();
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail_l(std::ostream &out) const final;
};
using CollectionNodeRef = CollectionNode::CollectionNodeRef;
}
