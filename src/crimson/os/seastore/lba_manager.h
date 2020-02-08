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
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

class LBATransaction {
public:
  virtual void set_block_offset(paddr_t addr) = 0;
  virtual ~LBATransaction() {}
};
using LBATransactionRef = std::unique_ptr<LBATransaction>;

class LBAPin;
using LBAPinRef = std::unique_ptr<LBAPin>;

class LBAPin {
public:
  virtual loff_t get_length() const = 0;
  virtual paddr_t get_paddr() const = 0;
  virtual laddr_t get_laddr() const = 0;

  virtual ~LBAPin() {}
};

using lba_pin_list_t = std::list<LBAPinRef>;
using lba_pin_ref_list_t = std::list<LBAPinRef&>;

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  using get_mapping_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_mapping_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mapping_ret get_mappings(
    laddr_t offset, loff_t length) = 0;

  using alloc_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_relative_ret = alloc_extent_relative_ertr::future<LBAPinRef>;
  virtual alloc_extent_relative_ret alloc_extent_relative(
    laddr_t hint,
    loff_t len,
    segment_off_t offset,
    LBATransaction &t) = 0;

  using set_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  using set_extent_ret = set_extent_ertr::future<LBAPinRef>;
  virtual set_extent_ret set_extent(
    laddr_t off, loff_t len, paddr_t addr,
    LBATransaction &t) = 0;

  using set_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  using set_extent_relative_ret = set_extent_ertr::future<LBAPinRef>;
  virtual set_extent_relative_ret set_extent_relative(
    laddr_t off, loff_t len, segment_off_t record_offset,
    LBATransaction &t) = 0;

  virtual void release_extent(LBAPinRef &ref, LBATransaction &t) = 0;

  using move_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using move_extent_relative_ret = move_extent_relative_ertr::future<LBAPinRef>;
  virtual move_extent_relative_ret move_extent_relative(
    LBAPinRef &ref,
    segment_off_t record_offset,
    LBATransaction &t) {
    release_extent(ref, t);
    return set_extent_relative(
      ref->get_laddr(),
      ref->get_length(),
      record_offset,
      t).handle_error(
	move_extent_relative_ertr::pass_further{},
	crimson::ct_error::invarg::handle([] {
	  throw std::runtime_error("Should be impossible");
	}));
  }

  using move_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using move_extent_ret = move_extent_ertr::future<LBAPinRef>;
  virtual move_extent_relative_ret move_extent(
    LBAPinRef &ref,
    laddr_t off, loff_t len, paddr_t addr,
    LBATransaction &t) {
    release_extent(ref, t);
    return set_extent(
      ref->get_laddr(),
      ref->get_length(),
      addr,
      t).handle_error(
	move_extent_relative_ertr::pass_further{},
	crimson::ct_error::invarg::handle([] {
	  throw std::runtime_error("Should be impossible");
	}));
  }

  using submit_lba_transaction_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using submit_lba_transaction_ret = submit_lba_transaction_ertr::future<>;
  virtual submit_lba_transaction_ret submit_lba_transaction(
    LBATransaction &t) = 0;

  virtual ~LBAManager() {}
};
using LBAManagerRef = std::unique_ptr<LBAManager>;

namespace lba_manager {
LBAManagerRef create_lba_manager(SegmentManager &segment_manager);
}

}
