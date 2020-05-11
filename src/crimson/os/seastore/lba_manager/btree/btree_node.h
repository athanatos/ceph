// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore::lba_manager::btree {

constexpr segment_off_t LBA_BLOCK_SIZE = 4096; // TODO

template <typename T>
struct node_iterator_t {
  T *node;
  uint16_t offset;
  node_iterator_t(
    T *parent,
    uint16_t offset) : node(parent), offset(offset) {}

  node_iterator_t(const node_iterator_t &) = default;
  node_iterator_t(node_iterator_t &&) = default;
  node_iterator_t &operator=(const node_iterator_t &) = default;
  node_iterator_t &operator=(node_iterator_t &&) = default;

  node_iterator_t &operator*() { return *this; }
  node_iterator_t *operator->() { return this; }

  node_iterator_t operator++(int) {
    auto ret = *this;
    ++offset;
    return ret;
  }

  node_iterator_t &operator++() {
    ++offset;
    return *this;
  }

  uint16_t operator-(const node_iterator_t &rhs) const {
    ceph_assert(rhs.node == node);
    return offset - rhs.offset;
  }

  node_iterator_t operator+(uint16_t off) const {
    return node_iterator_t(
      node,
      offset + off);
  }
  node_iterator_t operator-(uint16_t off) const {
    return node_iterator_t(
      node,
      offset - off);
  }

  bool operator==(const node_iterator_t &rhs) const {
    ceph_assert(node == rhs.node);
    return rhs.offset == offset;
  }

  bool operator!=(const node_iterator_t &rhs) const {
    return !(*this == rhs);
  }

  laddr_t get_lb() const {
    return node->get_lb(offset);
  }

  extent_len_t get_length() const {
    return node->get_length(offset);
  }

  void set_lb(laddr_t lb) {
    node->set_lb(offset, lb);
  }

  laddr_t get_ub() const {
    auto next = *this + 1;
    if (next == node->end())
      return L_ADDR_MAX;
    else
      return next->get_lb();
  }

  auto get_val() const {
    return node->get_val(offset);
  };

  template <typename U>
  void set_val(U val) {
    node->set_val(offset, val);
  }

  bool contains(laddr_t addr) {
    return (get_lb() <= addr) && (get_ub() > addr);
  }

  uint16_t get_offset() const {
    return offset;
  }

  char *get_key_ptr() {
    return node->get_key_ptr(offset);
  }

  char *get_val_ptr() {
    return node->get_val_ptr(offset);
  }
};

template <typename T>
struct LBANodeIterHelper {
  using internal_iterator_t = node_iterator_t<T>;
  internal_iterator_t begin() {
    return internal_iterator_t(
      static_cast<T*>(this),
      0);
  }
  internal_iterator_t end() {
    return internal_iterator_t(
      static_cast<T*>(this),
      static_cast<T*>(this)->get_size());
  }
  internal_iterator_t iter_idx(uint16_t off) {
    return internal_iterator_t(
      static_cast<T*>(this),
      off);
  }
  internal_iterator_t find(laddr_t l) {
    auto ret = begin();
    for (; ret != end(); ++ret) {
      if (ret->get_lb() == l)
	break;
    }
    return ret;
  }
  internal_iterator_t get_split_pivot() {
    return iter_idx(static_cast<T*>(this)->get_size() / 2);
  }

  void copy_from_foreign(
    internal_iterator_t tgt,
    internal_iterator_t from_src,
    internal_iterator_t to_src) {
    ceph_assert(tgt->node != from_src->node);
    ceph_assert(to_src->node == from_src->node);
    memcpy(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memcpy(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  void copy_from_local(
    internal_iterator_t tgt,
    internal_iterator_t from_src,
    internal_iterator_t to_src) {
    ceph_assert(tgt->node == from_src->node);
    ceph_assert(to_src->node == from_src->node);
    memmove(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memmove(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

};


template <typename T, typename C, typename P>
std::tuple<TCachedExtentRef<C>, TCachedExtentRef<C>, P>
do_make_split_children(
  T &parent,
  Cache &cache,
  Transaction &t)
{
  auto left = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);
  auto right = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);
  auto piviter = parent.get_split_pivot();

  left->copy_from_foreign(left->begin(), parent.begin(), piviter);
  left->set_size(piviter - parent.begin());

  right->copy_from_foreign(right->begin(), piviter, parent.end());
  right->set_size(parent.end() - piviter);

  return std::make_tuple(left, right, piviter->get_lb());
}

template <typename T, typename C>
TCachedExtentRef<C> do_make_full_merge(
  T &left,
  Cache &cache,
  Transaction &t,
  TCachedExtentRef<C> &_right)
{
  ceph_assert(_right->get_type() == T::type);
  T &right = *static_cast<T*>(_right.get());
  auto replacement = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);

  replacement->copy_from_foreign(
    replacement->end(),
    left.begin(),
    left.end());
  replacement->set_size(left.get_size());
  replacement->copy_from_foreign(
    replacement->end(),
    right.begin(),
    right.end());
  replacement->set_size(left.get_size() + right.get_size());
  return replacement;
}

template <typename T, typename C, typename P>
std::tuple<TCachedExtentRef<C>, TCachedExtentRef<C>, P>
do_make_balanced(
  T &left,
  Cache &cache,
  Transaction &t,
  TCachedExtentRef<C> &_right,
  P pivot,
  bool prefer_left)
{
  ceph_assert(_right->get_type() == T::type);
  T &right = *static_cast<T*>(_right.get());
  auto replacement_left = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);
  auto replacement_right = cache.alloc_new_extent<T>(
    t, LBA_BLOCK_SIZE);

  auto total = left.get_size() + right.get_size();
  auto pivot_idx = (left.get_size() + right.get_size()) / 2;
  if (total % 2 && prefer_left) {
    pivot_idx++;
  }
  auto replacement_pivot = pivot_idx > left.get_size() ?
    right.iter_idx(pivot_idx - left.get_size())->get_lb() :
    left.iter_idx(pivot_idx)->get_lb();

  if (pivot_idx < left.get_size()) {
    replacement_left->copy_from_foreign(
      replacement_left->end(),
      left.begin(),
      left.iter_idx(pivot_idx));
    replacement_left->set_size(pivot_idx);

    replacement_right->copy_from_foreign(
      replacement_right->end(),
      left.iter_idx(pivot_idx),
      left.end());

    replacement_right->set_size(left.get_size() - pivot_idx);
    replacement_right->copy_from_foreign(
      replacement_right->end(),
      right.begin(),
      right.end());
    replacement_right->set_size(total - pivot_idx);
  } else {
    replacement_left->copy_from_foreign(
      replacement_left->end(),
      left.begin(),
      left.end());
    replacement_left->set_size(left.get_size());

    replacement_left->copy_from_foreign(
      replacement_left->end(),
      right.begin(),
      right.iter_idx(left.get_size() - pivot_idx));
    replacement_left->set_size(pivot_idx);

    replacement_right->copy_from_foreign(
      replacement_right->end(),
      right.iter_idx(left.get_size() - pivot_idx),
      right.end());
    replacement_right->set_size(total - pivot_idx);
  }

  return std::make_tuple(
    replacement_left,
    replacement_right,
    replacement_pivot);
}

}
