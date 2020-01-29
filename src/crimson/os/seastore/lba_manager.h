// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class LBAPin;
using LBAPinRef = std::unique_ptr<LBAPin>;

class LBAPin {
  friend class lba_pin_split_merge;
protected:

  virtual bool can_merge(const LBAPin &right) const = 0;
  virtual LBAPinRef merge(LBAPin &&right) && = 0;
  virtual void split(uint64_t offset, uint64_t length) = 0;
public:

  virtual loff_t get_length() const = 0;
  virtual paddr_t get_paddr() const = 0;
  virtual laddr_t get_laddr() const = 0;

  virtual ~LBAPin() {}
};

struct lba_pin_split_merge {
  LBAPinRef split(
    uint64_t offset,
    uint64_t length,
    LBAPinRef &&pin) const {
    pin->split(offset, length);
    return std::move(pin);
  }
  bool can_merge(const LBAPinRef &left, const LBAPinRef &right) const {
    return left->can_merge(*right);
  }
  LBAPinRef merge(LBAPinRef &&left, LBAPinRef &&right) const {
    return std::move(*(right.release())).merge(std::move(*(left.release())));
  }
  uint64_t length(const LBAPinRef &b) const { return b->get_length(); }
};

using extent_set = interval_set<uint64_t>;
using extent_map = interval_map<uint64_t, LBAPinRef, lba_pin_split_merge>;

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
};

}
