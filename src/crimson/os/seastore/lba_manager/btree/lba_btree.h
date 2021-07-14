// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>

#include "crimson/common/log.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"

namespace crimson::os::seastore::lba_manager::btree {


class LBABtree {
  static constexpr size_t MAX_DEPTH = 16;
  lba_root_t root;

public:
  using base_iertr = LBAManager::base_iertr;

  template <bool is_const>
  class iter_t {
    template <typename NodeType>
    struct node_position_t {
      typename NodeType::Ref node;
      typename NodeType::template iter_t<is_const> position;
    };
    std::array<node_position_t<LBAInternalNode>, MAX_DEPTH> internal;
    node_position_t<LBALeafNode> leaf;
    
    iter_t(const std::enable_if<is_const, iter_t<!is_const>> &other)
      : internal(other.internal), leaf(other.leaf) {}

  public:
    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(is_const);
      return iter_t<!is_const>(*this);
    }

    using advance_iertr = base_iertr;
    using advance_ret = base_iertr::future<iter_t>;
    advance_ret next() {
      return advance_ret(
	interruptible::ready_future_marker{},
	*this);
    }

    // Work nicely with for loops without requiring a nested type.
    using reference = iter_t&;
    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    const laddr_t &get_key();
    const lba_map_val_t &get_val();
    bool is_end() const { return false; /* TODO */ }
  };

  LBABtree(lba_root_t root) : root(root) {}

  using iterator = iter_t<false>;
  using const_iterator = iter_t<true>;

  using bound_iertr = base_iertr;
  template <bool is_const>
  using bound_ret = base_iertr::future<iter_t<is_const>>;
  bound_ret<true> lower_bound(
    op_context_t c,
    laddr_t addr) const;
  bound_ret<false> lower_bound(
    op_context_t c,
    laddr_t addr);
  bound_ret<true> upper_bound(
    op_context_t c,
    laddr_t addr) const {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (!iter->is_end() && iter->get_key() == addr) {
	return iter.next();
      } else {
	return bound_ret<true>(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }
  bound_ret<false> upper_bound(
    op_context_t c,
    laddr_t addr) {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (!iter->is_end() && iter->get_key() == addr) {
	return iter.next();
      } else {
	return bound_ret<false>(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }

  /**
   * insert
   *
   * @param c [in] op context
   * @param iter [in] hint, insertion constant if immediately prior to iter
   * @param laddr [in] addr at which to insert
   * @param val [in] val to insert
   */
  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<>;
  insert_ret insert(
    op_context_t c,   ///< [in] op context
    iterator iter,    ///< [in] hint, insertion constant time if immediately after iter
    laddr_t laddr,
    lba_map_val_t val
  );
  insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val) {
    return upper_bound(
      c, laddr
    ).si_then([this, c, laddr, val](auto iter) {
      return insert(c, iter, laddr, val);
    });
  }


  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;

  using update_iertr = base_iertr;
  using update_ret = update_iertr::future<>;
};

}
