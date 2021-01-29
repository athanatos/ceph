// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "include/denc.h"

namespace crimson::os::seastore {

struct onode_layout_t {
  ceph_le32 size;
};

inline bool operator==(
  const onode_layout_t &lhs,
  const onode_layout_t &rhs) {
  return lhs.size == rhs.size;
}

/**
 * Onode
 *
 * Interface manipulated by seastore.  OnodeManager implementations should
 * return objects derived from this interface with layout referencing
 * internal representation of onode_layout_t.
 */
class Onode : public boost::intrusive_ref_counter<
  Onode,
  boost::thread_unsafe_counter>
{
  onode_layout_t *layout = nullptr;
public:
  Onode(onode_layout_t *layout)
    : layout(layout)
  {}

  size_t get_object_size() const {
    return layout->size;
  }

  size_t get_onode_ondisk_size() const {
    return 0; // TODO
  }

  template <typename F>
  void with_mutable(F &&f) {
    mark_mutable();
    f(*layout);
  }

  bool operator==(const Onode &rhs) const {
    return *layout == *rhs.layout;
  }

protected:

  /// Prepare underlying representation for mutation
  virtual void mark_mutable() = 0;
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}
