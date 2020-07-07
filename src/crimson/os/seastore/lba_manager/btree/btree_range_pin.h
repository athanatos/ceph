// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

class LBANode;
using LBANodeRef = TCachedExtentRef<LBANode>;

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

  bool is_root() const {
    return begin == 0 && end == L_ADDR_MAX;
  }
};

inline std::ostream &operator<<(
  std::ostream &lhs,
  const lba_node_meta_t &rhs)
{
  return lhs << "btree_node_meta_t("
	     << "begin=" << rhs.begin
	     << ", end=" << rhs.end
	     << ", depth=" << rhs.depth
	     << ")";
}


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

  static auto get_tuple(const lba_node_meta_t &meta) {
    return std::make_tuple(-meta.depth, meta.begin);
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
  btree_range_pin_t(CachedExtent *extent)
    : extent(extent) {}
  btree_range_pin_t(const btree_range_pin_t &rhs, CachedExtent *extent)
    : range(rhs.range), extent(extent) {}

  bool has_ref() const {
    return !!ref;
  }

  bool is_root() const {
    return range.is_root();
  }

  void set_range(const lba_node_meta_t &nrange) {
    range = nrange;
  }
  void set_extent(CachedExtent *nextent) {
    assert(!extent);
    extent = nextent;
  }

  friend bool operator<(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) < get_tuple(rhs.range);
  }
  friend bool operator>(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) > get_tuple(rhs.range);
  }
  friend bool operator==(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) == rhs.get_tuple(rhs.range);
  }

  struct meta_cmp_t {
    bool operator()(
      const btree_range_pin_t &lhs, const lba_node_meta_t &rhs) const {
      return get_tuple(lhs.range) < get_tuple(rhs);
    }
    bool operator()(
      const lba_node_meta_t &lhs, const btree_range_pin_t &rhs) const {
      return get_tuple(lhs) < get_tuple(rhs.range);
    }
  };

  ~btree_range_pin_t();
};

inline std::ostream &operator<<(
  std::ostream &lhs,
  const btree_range_pin_t &rhs)
{
  return lhs << "btree_range_pin_t("
	     << "begin=" << rhs.range.begin
	     << ", end=" << rhs.range.end
	     << ", depth=" << rhs.range.depth
	     << ", extent=" << rhs.extent
	     << ")";
}

class btree_pin_set_t {
  btree_range_pin_t::index_t pins;

  decltype(pins)::iterator get_iter(btree_range_pin_t &pin) {
    return decltype(pins)::s_iterator_to(pin);
  }

  void remove_pin(btree_range_pin_t &pin, bool check_parent);

public:
  btree_range_pin_t *maybe_get_parent(btree_range_pin_t &pin);

  btree_range_pin_t *maybe_get_first_child(btree_range_pin_t &pin);

  void add_pin(btree_range_pin_t &pin);

  void retire(btree_range_pin_t &pin);
  void check_parent(btree_range_pin_t &pin);

  void release_if_no_children(btree_range_pin_t &pin);

  ~btree_pin_set_t() {
    assert(pins.empty());
  }
};

struct BtreeLBAPin : LBAPin {
  paddr_t paddr;
  btree_range_pin_t pin;

public:
  BtreeLBAPin() = default;

  BtreeLBAPin(
    paddr_t paddr,
    lba_node_meta_t &&meta)
    : paddr(paddr) {
    pin.set_range(std::move(meta));
  }

  void link_extent(LogicalCachedExtent *ref) final {
    pin.set_extent(ref);
  }

  extent_len_t get_length() const final {
    assert(pin.range.end > pin.range.begin);
    return pin.range.end - pin.range.begin;
  }
  paddr_t get_paddr() const final {
    return paddr;
  }
  laddr_t get_laddr() const final {
    return pin.range.begin;
  }
  LBAPinRef duplicate() const final {
    auto ret = std::unique_ptr<BtreeLBAPin>(new BtreeLBAPin);
    ret->pin.set_range(pin.range);
    ret->paddr = paddr;
    return ret;
  }
};

}
