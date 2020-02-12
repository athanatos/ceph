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

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not result until all pins have resolved (set_paddr called)
   */
  using get_mapping_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_mapping_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mapping_ret get_mappings(
    laddr_t offset, loff_t length,
    Transaction &t) = 0;

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAPin.
   */
  using alloc_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_relative_ret = alloc_extent_relative_ertr::future<LBAPinRef>;
  virtual alloc_extent_relative_ret alloc_extent_relative(
    laddr_t hint,
    loff_t len,
    segment_off_t offset,
    Transaction &t) = 0;

  /**
   * Creates a new absolute mapping.
   *
   * off~len must be unreferenced
   */
  using set_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  using set_extent_ret = set_extent_ertr::future<LBAPinRef>;
  virtual set_extent_ret set_extent(
    laddr_t off, loff_t len, paddr_t addr,
    Transaction &t) = 0;

  /**
   * Creates a new relative mapping.
   *
   * off~len must be unreferenced
   */
  using set_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  using set_extent_relative_ret = set_extent_ertr::future<LBAPinRef>;
  virtual set_extent_relative_ret set_extent_relative(
    laddr_t off, loff_t len, segment_off_t record_offset,
    Transaction &t) = 0;

  /**
   * Decrements ref count on extent
   *
   * @return true if freed
   */
  virtual bool decref_extent(LBAPinRef &ref, Transaction &t) = 0;

  /**
   * Increments ref count on extent
   */
  virtual void incref_extent(LBAPinRef &ref, Transaction &t) = 0;

  /**
   * Moves mapping denoted by ref.
   *
   * ref must have only one refcount
   */
  using move_extent_relative_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using move_extent_relative_ret = move_extent_relative_ertr::future<LBAPinRef>;
  virtual move_extent_relative_ret move_extent_relative(
    LBAPinRef &ref,
    segment_off_t record_offset,
    Transaction &t) {
    bool freed = decref_extent(ref, t);
    ceph_assert(freed);
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

  /**
   * Moves mapping denoted by ref.
   *
   * ref must have only one refcount
   */
  using move_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using move_extent_ret = move_extent_ertr::future<LBAPinRef>;
  virtual move_extent_relative_ret move_extent(
    LBAPinRef &ref,
    laddr_t off, loff_t len, paddr_t addr,
    Transaction &t) {
    bool freed = decref_extent(ref, t);
    ceph_assert(freed);
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
    Transaction &t) = 0;

  virtual TransactionRef create_transaction() = 0;

  virtual ~LBAManager() {}
};
using LBAManagerRef = std::unique_ptr<LBAManager>;

class Cache;
namespace lba_manager {
LBAManagerRef create_lba_manager(
  SegmentManager &segment_manager,
  Cache &cache);
}

}
