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

class LBAPin {
public:
  virtual extent_len_t get_length() const = 0;
  virtual paddr_t get_paddr() const = 0;
  virtual laddr_t get_laddr() const = 0;

  virtual ~LBAPin() {}
};
using LBAPinRef = std::unique_ptr<LBAPin>;
std::ostream &operator<<(std::ostream &out, const LBAPin &rhs);

using lba_pin_list_t = std::list<LBAPinRef>;

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs);

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  using mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(
    Transaction &t
  ) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not result until all pins have resolved (set_paddr called)
   */
  using get_mapping_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
  using get_mapping_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset, extent_len_t length) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not result until all pins have resolved (set_paddr called)
   */
  using get_mappings_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_mappings_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&extent_lisk) = 0;

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAPin.
   */
  using alloc_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_ret = alloc_extent_ertr::future<LBAPinRef>;
  virtual alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) = 0;

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
    Transaction &t,
    laddr_t off, extent_len_t len, paddr_t addr) = 0;

  using ref_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  /**
   * Decrements ref count on extent
   *
   * @return true if freed
   */
  using decref_extent_ret = ref_ertr::future<bool>;
  virtual decref_extent_ret decref_extent(
    Transaction &t,
    LBAPin &ref) = 0;

  /**
   * Increments ref count on extent
   */
  using incref_extent_ret = ref_ertr::future<>;
  virtual incref_extent_ret incref_extent(
    Transaction &t,
    LBAPin &ref) = 0;

  /**
   * Moves mapping denoted by ref.
   *
   * ref must have only one refcount
   */
  using move_extent_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using move_extent_ret = move_extent_ertr::future<LBAPinRef>;
  virtual move_extent_ret move_extent(
    Transaction &t,
    LBAPin &ref,
    paddr_t addr) {
    return decref_extent(t, ref
    ).safe_then([this](auto freed) {
      ceph_assert(freed);
    }).safe_then([this, &t, &ref, addr] {
      return set_extent(
	t,
	ref.get_laddr(),
	ref.get_length(),
	addr).handle_error(
	  move_extent_ertr::pass_further{},
	  crimson::ct_error::invarg::handle([] {
	    throw std::runtime_error("Should be impossible");
	  }));
    });
  }

  // TODO: probably unused, removed
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
