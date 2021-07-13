// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>

#include "crimson/common/log.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"

class LBABtree {
  constexpr size_t MAX_DEPTH = 16;

  lba_root_t root;
public:
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
      static_assert(!is_const);
      return iter_t<!is_const>(*this);
    }

    // Work nicely with for loops without requiring a nested type.
    using reference = iter_t&;
    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    const laddr_t &get_key();
    const lba_map_val_t &get_val();
  };
  using iterator = iter_t<false>;
  using const_iterator = iter_t<true>;

  using lower_bound_iertr = base_iertr;
  template <bool is_const>
  using lower_bound_ret = base_iertr::future<iter_t<is_const>>;

  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<>;
  insert_ret insert(
    op_context_t c,
    iterator iter,
    laddr_t laddr,
    lba_map_val_t val);

  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<>;
  insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val);


  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;

  using update_iertr = base_iertr;
  using update_ret = update_iertr::future<>;
};


  LBABtree(lba_root_t root) : root(root) {}
};
