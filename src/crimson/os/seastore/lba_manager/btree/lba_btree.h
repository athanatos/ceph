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

  class iterator;
  using iterator_fut = base_iertr::future<iterator>;

  class iterator {
  public:
    iterator(const iterator &) noexcept = default;
    iterator(iterator &&) noexcept = default;
    iterator &operator=(const iterator &) = default;
    iterator &operator=(iterator &&) = default;

    iterator(depth_t depth) : internal(depth - 1) {}

    iterator_fut next(op_context_t c) const;
    iterator_fut prev(op_context_t c) const;

    void assert_valid() const {
      assert(leaf.node);
      assert(leaf.pos <= leaf.node->get_size());

      for (auto &i: internal) {
	assert(i.node);
	assert(i.pos < i.node->get_size());
      }
    }

    depth_t get_depth() const {
      return internal.size() + 1;
    }

    auto &get_internal(depth_t depth) {
      assert(depth > 1);
      assert((depth - 2) < internal.size());
      return internal[depth - 2];
    }

    const auto &get_internal(depth_t depth) const {
      assert(depth > 1);
      assert((depth - 2) < internal.size());
      return internal[depth - 2];
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
      assert(leaf.pos <= leaf.node->get_size());
      return leaf.pos == leaf.node->get_size();
    }

    bool is_begin() const {
      for (auto &i: internal) {
	if (i.pos != 0)
	  return false;
      }
      return leaf.pos == 0;
    }

    LBAPinRef get_pin() const {
      assert(!is_end());
      return std::make_unique<BtreeLBAPin>(
	leaf.node,
	get_val().paddr.maybe_relative_to(leaf.node->get_paddr()),
	lba_node_meta_t{ get_key(), get_key() + get_val().len, 0 });
    }

  private:
    iterator() = default;

    friend class LBABtree;
    static constexpr uint16_t INVALID = std::numeric_limits<uint16_t>::max();
    template <typename NodeType>
    struct node_position_t {
      typename NodeType::Ref node;
      uint16_t pos = INVALID;

      void reset() {
	*this = node_position_t{};
      }

      auto get_iter() {
	assert(pos != INVALID);
	assert(pos < node->get_size());
	return node->iter_idx(pos);
      }
    };
    boost::container::static_vector<
      node_position_t<LBAInternalNode>, MAX_DEPTH> internal;
    node_position_t<LBALeafNode> leaf;

    depth_t check_split() const {
      if (!leaf.node->at_max_capacity()) {
	return 0;
      }
      for (depth_t split_from = 1; split_from < get_depth(); ++split_from) {
	if (!get_internal(split_from + 1).node->at_max_capacity())
	  return split_from;
      }
      return get_depth();
    }

    depth_t check_merge() const {
      if (!leaf.node->at_min_capacity()) {
	return 0;
      }
      for (depth_t merge_from = 1; merge_from < get_depth(); ++merge_from) {
	if (!get_internal(merge_from + 1).node->at_min_capacity())
	  return merge_from;
      }
      return get_depth();
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
    ).si_then([this, c, addr](auto iter) {
      if (!iter.is_end() && iter.get_key() == addr) {
	return iter.next(c);
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
    ).si_then([this, c, addr](auto iter) {
      if (iter.is_begin()) {
	return iterator_fut(
	  interruptible::ready_future_marker{},
	  iter);
      } else {
	return iter.prev(
	  c
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
  static auto iterate_repeat(op_context_t c, iterator_fut &&iter_fut, F &&f) {
    return std::move(iter_fut).si_then([c, f=std::forward<F>(f)](auto iter) {
      return seastar::do_with(
	iter,
	std::move(f),
	[c](auto &pos, auto &f) {
	  return trans_intr::repeat(
	    [c, &f, &pos] {
	      return f(
		pos
	      ).si_then([c, &pos](auto done) {
		if (done == seastar::stop_iteration::yes) {
		  return iterate_repeat_ret(
		    interruptible::ready_future_marker{},
		    seastar::stop_iteration::yes);
		} else {
		  ceph_assert(!pos.is_end());
		  return pos.next(
		    c
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
   */
  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;
  remove_ret remove(
    op_context_t c,
    iterator iter);

  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from lba tree) and drops or initializes
   * accordingly.
   */
  using init_cached_extent_iertr = base_iertr;
  using init_cached_extent_ret = init_cached_extent_iertr::future<>;
  init_cached_extent_ret init_cached_extent(op_context_t c, CachedExtentRef e);
private:
  lba_root_t root;
  bool root_dirty = false;

  using get_internal_node_iertr = base_iertr;
  using get_internal_node_ret = get_internal_node_iertr::future<LBAInternalNodeRef>;
  static get_internal_node_ret get_internal_node(
    op_context_t c,
    depth_t depth,
    paddr_t offset);

  using get_leaf_node_iertr = base_iertr;
  using get_leaf_node_ret = get_leaf_node_iertr::future<LBALeafNodeRef>;
  static get_leaf_node_ret get_leaf_node(
    op_context_t c,
    paddr_t offset);

  using lookup_root_iertr = base_iertr;
  using lookup_root_ret = lookup_root_iertr::future<>;
  lookup_root_ret lookup_root(op_context_t c, iterator &iter) const {
    if (root.get_depth() > 1) {
      return get_internal_node(
	c,
	root.get_depth(),
	root.get_location()
      ).si_then([this, &iter](LBAInternalNodeRef root_node) {
	iter.get_internal(root.get_depth()).node = root_node;
	return lookup_root_iertr::now();
      });
    } else {
      return get_leaf_node(
	c,
	root.get_location()
      ).si_then([this, &iter](LBALeafNodeRef root_node) {
	iter.leaf.node = root_node;
	return lookup_root_iertr::now();
      });
    }
  }

  using lookup_internal_level_iertr = base_iertr;
  using lookup_internal_level_ret = lookup_internal_level_iertr::future<>;
  template <typename F>
  static lookup_internal_level_ret lookup_internal_level(
    op_context_t c,
    depth_t depth,
    iterator &iter,
    F &f) {
    assert(depth > 1);
    auto &parent_entry = iter.get_internal(depth + 1);
    auto parent = parent_entry.node;
    auto node_iter = parent->iter_idx(parent_entry.pos);
    return get_internal_node(
      c,
      depth,
      node_iter->get_val().maybe_relative_to(parent->get_paddr())
    ).si_then([c, depth, &iter, &f](LBAInternalNodeRef node) {
      auto &entry = iter.get_internal(depth);
      entry.node = node;
      auto node_iter = f(*node);
      assert(node_iter != node->end());
      entry.pos = node_iter->get_offset();
      return seastar::now();
    });
  }

  using lookup_leaf_iertr = base_iertr;
  using lookup_leaf_ret = lookup_leaf_iertr::future<>;
  template <typename F>
  static lookup_internal_level_ret lookup_leaf(
    op_context_t c,
    iterator &iter,
    F &f
  ) {
    auto &parent_entry = iter.get_internal(2);
    auto parent = parent_entry.node;
    assert(parent);
    auto node_iter = parent->iter_idx(parent_entry.pos);

    return get_leaf_node(
      c,
      node_iter->get_val().maybe_relative_to(parent->get_paddr())
    ).si_then([c, &iter, &f](LBALeafNodeRef node) {
      iter.leaf.node = node;
      auto node_iter = f(*node);
      iter.leaf.pos = node_iter->get_offset();
      return seastar::now();
    });
  }

  using lookup_depth_range_iertr = base_iertr;
  using lookup_depth_range_ret = lookup_depth_range_iertr::future<>;
  template <typename LI, typename LL>
  static lookup_depth_range_ret lookup_depth_range(
    op_context_t c, ///< [in] context
    iterator &iter, ///< [in,out] iterator to populate
    depth_t from,   ///< [in] from inclusive
    depth_t to,     ///< [in] to exclusive, (to <= from, to == from is a noop)
    LI &li,         ///< [in] internal->iterator
    LL &ll          ///< [in] leaf->iterator
  ) {
    LOG_PREFIX(LBATree::lookup_depth_range);
    DEBUGT("{} -> {}", c.trans, from, to);
    return seastar::do_with(
      from,
      [FNAME, c, to, &iter, &li, &ll](auto &d) {
	return trans_intr::repeat(
	  [FNAME, c, to, &iter, &li, &ll, &d] {
	    if (d > to) {
	      return [&] {
		if (d > 1) {
		  return lookup_internal_level(
		    c,
		    d,
		    iter,
		    li);
		} else {
		  assert(d == 1);
		  return lookup_leaf(
		    c,
		    iter,
		    ll);
		}
	      }().si_then([&d] {
		--d;
		return lookup_depth_range_iertr::make_ready_future<
		  seastar::stop_iteration
		  >(seastar::stop_iteration::no);
	      });
	    } else {
	      return lookup_depth_range_iertr::make_ready_future<
		seastar::stop_iteration
		>(seastar::stop_iteration::yes);
	    }
	  });
      });
#if 0
    // Causing mysterious crash, probably a bug in do_for_each itself TODO
    return trans_intr::do_for_each(
      boost::reverse_iterator(boost::counting_iterator(from)),
      boost::reverse_iterator(boost::counting_iterator(to)),
      [FNAME, c, &iter, &li, &ll](auto d) {
	DEBUGT("depth {}", c.trans, d);
	if (d > 1) {
	  return lookup_internal_level(
	    c,
	    d,
	    iter,
	    li);
	} else if (d == 1) {
	  return lookup_leaf(
	    c,
	    iter,
	    ll);
	} else {
	  assert(0 == "impossible");
	}
      });
#endif
  }

  using lookup_iertr = base_iertr;
  using lookup_ret = lookup_iertr::future<iterator>;
  template <typename LI, typename LL>
  lookup_ret lookup(
    op_context_t c,
    LI &&lookup_internal,
    LL &&lookup_leaf) const {
    LOG_PREFIX(LBATree::lookup);
    return seastar::do_with(
      iterator{root.get_depth()},
      std::forward<LI>(lookup_internal),
      std::forward<LL>(lookup_leaf),
      [FNAME, this, c](auto &iter, auto &li, auto &ll) {
	return lookup_root(
	  c, iter
	).si_then([FNAME, this, c, &iter, &li, &ll] {
	  if (iter.get_depth() > 1) {
	    auto &root_entry = *(iter.internal.rbegin());
	    root_entry.pos = li(*(root_entry.node)).get_offset();
	  } else {
	    auto &root_entry = iter.leaf;
	    auto riter = ll(*(root_entry.node));
	    root_entry.pos = riter->get_offset();
	  }
	  DEBUGT("got root, depth {}", c.trans, root.get_depth());
	  return lookup_depth_range(
	    c,
	    iter,
	    root.get_depth() - 1,
	    0,
	    li,
	    ll);
	}).si_then([c, &iter] {
	  return std::move(iter);
	});
      });
  }

  using find_insertion_iertr = base_iertr;
  using find_insertion_ret = find_insertion_iertr::future<>;
  static find_insertion_ret find_insertion(
    op_context_t c,
    laddr_t laddr,
    iterator &iter);

  using handle_split_iertr = base_iertr;
  using handle_split_ret = handle_split_iertr::future<>;
  handle_split_ret handle_split(
    op_context_t c,
    iterator &iter);

  using handle_merge_iertr = base_iertr;
  using handle_merge_ret = handle_merge_iertr::future<>;
  handle_merge_ret handle_merge(
    op_context_t c,
    iterator &iter);

  template <typename T>
  using node_position_t = iterator::node_position_t<T>;

  template <typename NodeType>
  friend base_iertr::future<typename NodeType::Ref> get_node(
    op_context_t c,
    depth_t depth,
    paddr_t addr);

  template <typename NodeType>
  friend handle_merge_ret merge_level(
    op_context_t c,
    depth_t depth,
    node_position_t<LBAInternalNode> &parent_pos,
    node_position_t<NodeType> &pos);
};

}
