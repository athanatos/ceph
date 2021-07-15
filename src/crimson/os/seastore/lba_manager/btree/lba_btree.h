// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/container/static_vector.hpp>
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
  bool root_dirty = false;

public:
  using base_iertr = LBAManager::base_iertr;

  template <bool is_const>
  class iter_t {
    friend class LBABtree;
    template <typename NodeType>
    struct node_position_t {
      typename NodeType::Ref node;
      typename NodeType::template iter_t<is_const> position;
    };
    boost::container::static_vector<
      node_position_t<LBAInternalNode>, MAX_DEPTH> internal;
    node_position_t<LBALeafNode> leaf;
    
    iter_t() = default;
    iter_t(const std::enable_if<is_const, iter_t<!is_const>> &other) noexcept
      : internal(other.internal), leaf(other.leaf) {}

  public:
    iter_t(const iter_t &) noexcept = default;
    iter_t(iter_t &&) noexcept = default;
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
  using iterator_fut = base_iertr::future<iterator>;
  using const_iterator_fut = base_iertr::future<const_iterator>;

  bool is_root_dirty() const {
    return root_dirty;
  }
  lba_root_t get_root_undirty() {
    ceph_assert(root_dirty);
    root_dirty = false;
    return root;
  }

  /// mkfs
  using mkfs_ret = lba_root_t;
  static mkfs_ret mkfs(op_context_t c);

  /**
   * lower_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator >= key
   */
  const_iterator_fut lower_bound(
    op_context_t c,
    laddr_t addr) const;
  iterator_fut lower_bound(
    op_context_t c,
    laddr_t addr);

  /**
   * upper_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator > key
   */
  const_iterator_fut upper_bound(
    op_context_t c,
    laddr_t addr
  ) const {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (!iter->is_end() && iter->get_key() == addr) {
	return iter.next();
      } else {
	return const_iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }
  iterator_fut upper_bound(
    op_context_t c,
    laddr_t addr) {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (!iter->is_end() && iter->get_key() == addr) {
	return iter.next();
      } else {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }
  iterator_fut begin(op_context_t c) {
    return lower_bound(c, 0);
  }
  const_iterator_fut begin(op_context_t c) const {
    return lower_bound(c, 0);
  }
  iterator_fut end(op_context_t c) {
    return upper_bound(c, L_ADDR_MAX);
  }
  const_iterator_fut end(op_context_t c) const {
    return upper_bound(c, L_ADDR_MAX);
  }

  /**
   * insert
   *
   * Inserts val at laddr with iter as a hint.  If element at laddr already
   * exists returns iterator to that element and does nothing.
   *
   * @param c [in] op context
   * @param iter [in] hint, insertion constant if immediately prior to iter
   * @param laddr [in] addr at which to insert
   * @param val [in] val to insert
   * @return pair<iter, bool> where iter points to element at addr, bool true
   *         iff element at laddr did not exist.
   */
  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<std::pair<iterator, bool>>;
  insert_ret insert(
    op_context_t c,
    iterator iter,
    laddr_t laddr,
    lba_map_val_t val
  );
  insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val) {
    return lower_bound(
      c, laddr
    ).si_then([this, c, laddr, val](auto iter) {
      return insert(c, iter, laddr, val);
    });
  }

  /**
   * update
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to update, must not be end
   * @param val [in] val with which to update
   */
  using update_iertr = base_iertr;
  using update_ret = update_iertr::future<>;
  update_ret update(
    op_context_t c,
    iterator iter,
    lba_map_val_t val);

  /**
   * remove
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to remove, must not be end
   */
  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;
  update_ret remove(
    op_context_t c,
    iterator iter);
};

}
