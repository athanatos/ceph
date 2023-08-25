// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <set>

#include "include/ceph_assert.h"

template <typename T>
class SyncReserver {
  const unsigned limit;
  std::set<T> reserved;

public:
  SyncReserver(unsigned limit) : limit(limit) {}

  /// Attempt to reserve t, returns true iff successful
  bool try_reserve(T &&t) {
    if (reserved.size() > limit) {
      return false;
    } else {
      ceph_assert(!reserved.count(t));
      reserved.insert(t);
      return true;
    }
  }

  /// Release reservation for t, may or may not be reserved
  void release(T &&t) {
    reserved.erase(t);
  }
};
