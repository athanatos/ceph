// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "include/byteorder.h"

#include "crimson/common/layout.h"

namespace crimson::common {

template <
  size_t CAPACITY,
  typename K,
  typename KINT,
  typename V,
  typename VINT>
class FixedKVNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, KINT, VINT>;
  L layout;

public:
  struct fixed_node_iter_t {
    friend class FixedKVNodeLayout;
    FixedKVNodeLayout *node;
    uint16_t offset;

    fixed_node_iter_t(
      FixedKVNodeLayout *parent,
      uint16_t offset) : node(parent), offset(offset) {}

    fixed_node_iter_t(const fixed_node_iter_t &) = default;
    fixed_node_iter_t(fixed_node_iter_t &&) = default;
    fixed_node_iter_t &operator=(const fixed_node_iter_t &) = default;
    fixed_node_iter_t &operator=(fixed_node_iter_t &&) = default;

    fixed_node_iter_t &operator*() { return *this; }
    fixed_node_iter_t *operator->() { return this; }

    fixed_node_iter_t operator++(int) {
      auto ret = *this;
      ++offset;
      return ret;
    }

    fixed_node_iter_t &operator++() {
      ++offset;
      return *this;
    }

    uint16_t operator-(const fixed_node_iter_t &rhs) const {
      ceph_assert(rhs.node == node);
      return offset - rhs.offset;
    }

    fixed_node_iter_t operator+(uint16_t off) const {
      return fixed_node_iter_t(
	node,
	offset + off);
    }
    fixed_node_iter_t operator-(uint16_t off) const {
      return fixed_node_iter_t(
	node,
	offset - off);
    }

    bool operator==(const fixed_node_iter_t &rhs) const {
      assert(node == rhs.node);
      return rhs.offset == offset;
    }

    bool operator!=(const fixed_node_iter_t &rhs) const {
      return !(*this == rhs);
    }

    K get_lb() const {
      return K(node->get_key_ptr()[offset]);
    }

    void set_lb(K lb) {
      node->get_key_ptr()[offset] = KINT(lb);
    }

    K get_ub() const {
      auto next = *this + 1;
      if (next == node->end())
	return std::numeric_limits<K>::max();
      else
	return next->get_lb();
    }

    V get_val() const {
      return V(node->get_val_ptr()[offset]);
    };

    void set_val(V val) {
      node->get_val_ptr()[offset] = VINT(val);
    }

    bool contains(K addr) {
      return (get_lb() <= addr) && (get_ub() > addr);
    }

    uint16_t get_offset() const {
      return offset;
    }

  private:
    char *get_key_ptr() {
      return reinterpret_cast<char *>(node->get_key_ptr() + offset);
    }

    char *get_val_ptr() {
      return reinterpret_cast<char *>(node->get_val_ptr() + offset);
    }
  };

public:
  FixedKVNodeLayout(char *buf) :
    buf(buf), layout(1, CAPACITY, CAPACITY) {}

  fixed_node_iter_t begin() {
    return fixed_node_iter_t(
      this,
      0);
  }

  fixed_node_iter_t end() {
    return fixed_node_iter_t(
      this,
      get_size());
  }

  fixed_node_iter_t iter_idx(uint16_t off) {
    return fixed_node_iter_t(
      this,
      off);
  }

  fixed_node_iter_t find(K l) {
    auto ret = begin();
    for (; ret != end(); ++ret) {
      if (ret->get_lb() == l)
	break;
    }
    return ret;
  }

  fixed_node_iter_t get_split_pivot() {
    return iter_idx(get_size() / 2);
  }

private:
  KINT *get_key_ptr() {
    return layout.template Pointer<1>(buf);
  }

  VINT *get_val_ptr() {
    return layout.template Pointer<2>(buf);
  }

public:
  uint16_t get_size() const {
    return *layout.template Pointer<0>(buf);
  }

  void set_size(uint16_t size) {
    *layout.template Pointer<0>(buf) = size;
  }

  size_t get_capacity() const {
    return CAPACITY;
  }

  void copy_from_foreign(
    fixed_node_iter_t tgt,
    fixed_node_iter_t from_src,
    fixed_node_iter_t to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    memcpy(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memcpy(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  void copy_from_local(
    fixed_node_iter_t tgt,
    fixed_node_iter_t from_src,
    fixed_node_iter_t to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    memmove(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memmove(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  K split_into(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right) {
    auto piviter = get_split_pivot();

    left.copy_from_foreign(left.begin(), begin(), piviter);
    left.set_size(piviter - begin());

    right.copy_from_foreign(right.begin(), piviter, end());
    right.set_size(end() - piviter);

    return piviter->get_lb();
  }

  void merge_from(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right)
  {
    copy_from_foreign(
      end(),
      left.begin(),
      left.end());
    set_size(left.get_size());
    copy_from_foreign(
      end(),
      right.begin(),
      right.end());
    set_size(left.get_size() + right.get_size());
  }

  static K balance_into_new_nodes(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right,
    bool prefer_left,
    FixedKVNodeLayout &replacement_left,
    FixedKVNodeLayout &replacement_right)
  {
    auto total = left.get_size() + right.get_size();
    auto pivot_idx = (left.get_size() + right.get_size()) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }
    auto replacement_pivot = pivot_idx > left.get_size() ?
      right.iter_idx(pivot_idx - left.get_size())->get_lb() :
      left.iter_idx(pivot_idx)->get_lb();

    if (pivot_idx < left.get_size()) {
      replacement_left.copy_from_foreign(
	replacement_left.end(),
	left.begin(),
	left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign(
	replacement_right.end(),
	left.iter_idx(pivot_idx),
	left.end());

      replacement_right.set_size(left.get_size() - pivot_idx);
      replacement_right.copy_from_foreign(
	replacement_right.end(),
	right.begin(),
	right.end());
      replacement_right.set_size(total - pivot_idx);
    } else {
      replacement_left.copy_from_foreign(
	replacement_left.end(),
	left.begin(),
	left.end());
      replacement_left.set_size(left.get_size());

      replacement_left.copy_from_foreign(
	replacement_left.end(),
	right.begin(),
	right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign(
	replacement_right.end(),
	right.iter_idx(pivot_idx - left.get_size()),
	right.end());
      replacement_right.set_size(total - pivot_idx);
    }

    return replacement_pivot;
  }
};

}
