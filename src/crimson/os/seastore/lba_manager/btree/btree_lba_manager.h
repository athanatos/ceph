// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore::lba_manager::btree {

class BtreeLBATransaction : LBATransaction {
public:
  void set_block_offset(paddr_t addr) final {}
  ~BtreeLBATransaction() final {}
};
using BtreeLBATransactionRef =
  std::unique_ptr<BtreeLBATransaction>;

class BtreeLBAPin;
using BtreeLBAPinRef = std::unique_ptr<LBAPin>;

class BtreeLBAPin : LBAPin {
  paddr_t paddr;
  laddr_t laddr;
  loff_t length;
public:
  loff_t get_length() const final {
    return length;
  }
  paddr_t get_paddr() const final {
    return paddr;
  }
  laddr_t get_laddr() const final {
    return laddr;
  }

  ~BtreeLBAPin() final = default;
};


class BtreeLBAManager : public LBAManager {
  SegmentManager &segment_manager;
public:
  BtreeLBAManager(SegmentManager &segment_manager);

  get_mapping_ret get_mappings(
    laddr_t offset, loff_t length) final;

  alloc_extent_relative_ret alloc_extent_relative(
    laddr_t hint,
    loff_t len,
    segment_off_t offset,
    LBATransaction &t) final;

  set_extent_ret set_extent(
    laddr_t off, loff_t len, paddr_t addr,
    LBATransaction &t) final;

  set_extent_relative_ret set_extent_relative(
    laddr_t off, loff_t len, segment_off_t record_offset,
    LBATransaction &t) final;

  void release_extent(LBAPinRef &ref, LBATransaction &t) final;

  submit_lba_transaction_ret submit_lba_transaction(
    LBATransaction &t) final;

};
  
}
