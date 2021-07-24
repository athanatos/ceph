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
public:
  using base_iertr = LBAManager::base_iertr;

  class iterator {
    friend class LBABtree;
    static constexpr uint16_t MAX = std::numeric_limits<uint16_t>::max();
    template <typename NodeType>
    struct node_position_t {
      typename NodeType::Ref node;
      uint16_t pos = MAX;
    };
    boost::container::static_vector<
      node_position_t<LBAInternalNode>, MAX_DEPTH> internal;
    node_position_t<LBALeafNode> leaf;
    
    iterator() = default;

  public:
    iterator(const iterator &) noexcept = default;
    iterator(iterator &&) noexcept = default;
    iterator &operator=(const iterator &) = default;
    iterator &operator=(iterator &&) = default;

    using next_iertr = base_iertr;
    using next_ret = base_iertr::future<iterator>;
    next_ret next() const {
      // TODOSAM
      return next_ret(
	interruptible::ready_future_marker{},
	*this);
    }

    using prev_iertr = base_iertr;
    using prev_ret = base_iertr::future<iterator>;
    prev_ret prev() const {
      // TODOSAM
      return prev_ret(
	interruptible::ready_future_marker{},
	*this);
    }

    laddr_t get_key() const {
      assert(!is_end());
      return leaf.node->iter_idx(leaf.pos).get_key();
    }
    lba_map_val_t get_val() const {
      assert(!is_end());
      return leaf.node->iter_idx(leaf.pos).get_val();
    }

    bool is_end() const {
      return leaf.pos == MAX;
    }

    bool is_begin() const {
      for (auto &i: internal) {
	if (i.pos != 0)
	  return false;
      }
      return leaf.pos == MAX;
    }

    LBAPinRef get_pin() const {
      assert(!is_end());
      return std::make_unique<BtreeLBAPin>(
	leaf.node,
	get_val().paddr /* probably needs to be adjusted TODO */,
	lba_node_meta_t{ get_key(), get_key() + get_val().len, 0 });
    }
  };

  LBABtree(lba_root_t root) : root(root) {}

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

  using iterator_fut = base_iertr::future<iterator>;

  /**
   * lower_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator >= key
   */
  iterator_fut lower_bound(
    op_context_t c,
    laddr_t addr) const;

  /**
   * upper_bound
   *
   * @param c [in] context
   * @param addr [in] ddr
   * @return least iterator > key
   */
  iterator_fut upper_bound(
    op_context_t c,
    laddr_t addr
  ) const {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (!iter.is_end() && iter.get_key() == addr) {
	return iter.next();
      } else {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      }
    });
  }

  /**
   * upper_bound_right
   *
   * @param c [in] context
   * @param addr [in] addr
   * @return least iterator i s.t. i.get_key() + i.get_val().len > key
   */
  iterator_fut upper_bound_right(
    op_context_t c,
    laddr_t addr) const
  {
    return lower_bound(
      c, addr
    ).si_then([this, addr](auto iter) {
      if (iter.is_begin()) {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      } else {
	return iter.prev(
	).si_then([iter, addr](auto prev) {
	  if ((prev.get_key() + prev.get_val().len) > addr) {
	    return iterator_fut(
	      interruptible::ready_future_marker{},
	      prev);
	  } else {
	    return iterator_fut(
	      interruptible::ready_future_marker{},
	      iter);
	  }
	});
      }
    });
  }

  iterator_fut begin(op_context_t c) const {
    return lower_bound(c, 0);
  }
  iterator_fut end(op_context_t c) const {
    return upper_bound(c, L_ADDR_MAX);
  }

  using iterate_repeat_ret = base_iertr::future<
    seastar::stop_iteration>;
  template <typename F>
  static auto iterate_repeat(iterator_fut &&iter_fut, F &&f) {
    return std::move(iter_fut).si_then([f=std::forward<F>(f)](auto iter) {
      return seastar::do_with(
	iter,
	[f=std::move(f)](auto &pos) {
	  return trans_intr::repeat(
	    [&f, &pos] {
	      return f(
		pos
	      ).si_then([&pos](auto done) {
		if (done == seastar::stop_iteration::yes) {
		  return iterate_repeat_ret(
		    interruptible::ready_future_marker{},
		    seastar::stop_iteration::yes);
		} else {
		  ceph_assert(!pos.is_end());
		  return pos.next(
		  ).si_then([&pos](auto next) {
		    pos = next;
		    return iterate_repeat_ret(
		      interruptible::ready_future_marker{},
		      seastar::stop_iteration::no);
		  });
		}
	      });
	    });
	});
    });
  }

  /**
   * insert
   *
   * Inserts val at laddr with iter as a hint.  If element at laddr already
   * exists returns iterator to that element unchanged and returns false.
   *
   * Invalidates all outstanding iterators for this tree on this transaction.
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
   * Invalidates all outstanding iterators for this tree on this transaction.
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to update, must not be end
   * @param val [in] val with which to update
   * @return iterator to newly updated element
   */
  using update_iertr = base_iertr;
  using update_ret = update_iertr::future<iterator>;
  update_ret update(
    op_context_t c,
    iterator iter,
    lba_map_val_t val);

  /**
   * remove
   *
   * Invalidates all outstanding iterators for this tree on this transaction.
   *
   * @param c [in] op context
   * @param iter [in] iterator to element to remove, must not be end
   * @return iterator to position after removed element
   */
  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<iterator>;
  update_ret remove(
    op_context_t c,
    iterator iter);

private:
  lba_root_t root;
  bool root_dirty = false;

  using get_internal_node_iertr = base_iertr;
  using get_internal_node_ret = get_internal_node_iertr::future<LBAInternalNodeRef>;
  get_internal_node_ret get_internal_node(
    op_context_t c,
    LBAInternalNodeRef parent,
    depth_t depth,
    paddr_t offset,
    paddr_t base);

  using lookup_internal_level_iertr = base_iertr;
  using lookup_internal_level_ret = lookup_internal_level_iertr::future<>;
  template <typename F>
  static lookup_internal_level_ret lookup_internal_level(
    op_context_t c,
    depth_t depth,
    iterator &iter,
    F &&f) {
    assert(iter.internal.size() > depth);
    assert(iter.internal[depth].node);
    auto parent = iter.internal[depth].node;
    auto node_iter = f(*parent);
    return get_internal_node_ret(
      c,
      parent,
      depth,
      node_iter->get_val().paddr,
      parent->get_paddr()
    ).si_then([c, depth, &iter](LBAInternalNodeRef node) {
      return seastar::now();
    });
  }
};

}
