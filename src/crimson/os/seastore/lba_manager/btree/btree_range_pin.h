// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

struct lba_node_meta_t {
  laddr_t begin = 0;
  laddr_t end = 0;
  depth_t depth = 0;

  bool is_parent_of(const lba_node_meta_t &other) {
    return (depth == other.depth + 1) &&
      (begin <= other.begin) &&
      (end >= other.end);
  }

  std::pair<lba_node_meta_t, lba_node_meta_t> split_into(laddr_t pivot) const {
    return std::make_pair(
      lba_node_meta_t{begin, pivot, depth},
      lba_node_meta_t{pivot, end, depth});
  }

  static lba_node_meta_t merge_from(const lba_node_meta_t &lhs, const lba_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return lba_node_meta_t{lhs.begin, rhs.end, lhs.depth};
  }

  static std::pair<lba_node_meta_t, lba_node_meta_t>
  rebalance(const lba_node_meta_t &lhs, const lba_node_meta_t &rhs, laddr_t pivot) {
    assert(lhs.depth == rhs.depth);
    return std::make_pair(
      lba_node_meta_t{lhs.begin, pivot, lhs.depth},
      lba_node_meta_t{pivot, rhs.end, lhs.depth});
  }
};

class btree_pin_set_t;
class btree_range_pin_t : public boost::intrusive::set_base_hook<> {
  friend class btree_pin_set_t;
  lba_node_meta_t range;

  btree_pin_set_t *pins = nullptr;

  // We need to be able to remember extent without holding a reference,
  // but we can do it more compactly -- TODO
  CachedExtent *extent = nullptr;
  CachedExtentRef ref;

  using index_t = boost::intrusive::set<btree_range_pin_t>;

  auto get_tuple() const {
    return std::make_tuple(range.begin, -range.depth);
  }

  void acquire_ref() {
    ref = CachedExtentRef(extent);
  }

  void drop_ref() {
    ref = CachedExtentRef();
  }

  bool is_parent_of(const btree_range_pin_t &other) {
    return range.is_parent_of(other.range);
  }

public:
  btree_range_pin_t() = default;
  void init(CachedExtent *extent, const lba_node_meta_t &nrange) {
    range = nrange;
  }

  friend bool operator<(const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return lhs.get_tuple() < rhs.get_tuple();
  }
  friend bool operator>(const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return lhs.get_tuple() > rhs.get_tuple();
  }
  friend bool operator==(const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return lhs.get_tuple() == rhs.get_tuple();
  }

  ~btree_range_pin_t();
};

class btree_pin_set_t {
  btree_range_pin_t::index_t pins;

  decltype(pins)::iterator get_iter(btree_range_pin_t &pin) {
    return decltype(pins)::s_iterator_to(pin);
  }

  void remove_pin(btree_range_pin_t &pin) {
    assert(pin.is_linked());

    auto parent = get_parent(pin);

    pins.erase(pin);
    pin.pins = nullptr;

    if (parent) {
      release_if_no_children(*parent);
    }
  }

public:
  btree_range_pin_t *get_parent(btree_range_pin_t &pin) {
    assert(pin.is_linked());
    auto iter = get_iter(pin);
    if (iter == pins.begin())
      return nullptr;
    --iter;
    if (iter->is_parent_of(pin)) {
      return &*iter;
    } else {
      return nullptr;
    }
  }

  btree_range_pin_t *get_child(btree_range_pin_t &pin) {
    assert(pin.is_linked());
    auto iter = get_iter(pin);
    ++iter;
    if (iter == pins.end())
      return nullptr;
    if (pin.is_parent_of(*iter)) {
      return &*iter;
    } else {
      return nullptr;
    }
  }

  void add_pin(btree_range_pin_t &pin, bool is_root) {
    assert(!pin.is_linked());
    pins.insert(pin);
    pin.pins = this;
    if (!is_root) {
      auto parent = get_parent(pin);
      assert(parent);
      parent->acquire_ref();
    }
    if (get_child(pin)) {
      pin.acquire_ref();
    }
  }

  void retire(btree_range_pin_t &pin) {
    pin.drop_ref();
    remove_pin(pin);
  }

  void release_if_no_children(btree_range_pin_t &pin) {
    assert(pin.is_linked());
    auto iter = get_iter(pin);
    iter++;
    if (iter == pins.end() || !pin.is_parent_of(*iter)) {
      pin.drop_ref();
    }
  }

  ~btree_pin_set_t() {
    assert(pins.empty());
  }
};

inline btree_range_pin_t::~btree_range_pin_t() {
  assert(!pins == !is_linked());
  assert(!ref);
  if (pins) {
    pins->remove_pin(*this);
  }
  extent = nullptr;
}

/* BtreeLBAPin
 *
 * References leaf node
 *
 * TODO: does not at this time actually keep the relevant
 * leaf resident in memory.  This is actually a bit tricky
 * as we can mutate and therefore replace a leaf referenced
 * by other, uninvolved but cached extents.  Will need to
 * come up with some kind of pinning mechanism that handles
 * that well.
 */
struct BtreeLBAPin : LBAPin {
  paddr_t paddr;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t length = 0;
  unsigned refcount = 0;

public:
  BtreeLBAPin(
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t length)
    : paddr(paddr), laddr(laddr), length(length) {}

  extent_len_t get_length() const final {
    return length;
  }
  paddr_t get_paddr() const final {
    return paddr;
  }
  laddr_t get_laddr() const final {
    return laddr;
  }
  LBAPinRef duplicate() const final {
    return LBAPinRef(new BtreeLBAPin(*this));
  }
};

}
