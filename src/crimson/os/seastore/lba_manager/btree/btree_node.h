// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/ceph_assert.h"

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
      ceph_assert(node == rhs.node);
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
    fixed_node_iter_t tgt,
    fixed_node_iter_t from_src,
    fixed_node_iter_t to_src) {
    ceph_assert(tgt->node == from_src->node);
    ceph_assert(to_src->node == from_src->node);
    memmove(
      tgt->get_val_ptr(), from_src->get_val_ptr(),
      to_src->get_val_ptr() - from_src->get_val_ptr());
    memmove(
      tgt->get_key_ptr(), from_src->get_key_ptr(),
      to_src->get_key_ptr() - from_src->get_key_ptr());
  }

  K fill_split_children(
    FixedKVNodeLayout &left,
    FixedKVNodeLayout &right) {
    auto piviter = get_split_pivot();

    left.copy_from_foreign(left.begin(), begin(), piviter);
    left.set_size(piviter - begin());

    right.copy_from_foreign(right.begin(), piviter, end());
    right.set_size(end() - piviter);

    return piviter->get_lb();
  }
};

}
