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

#define LBA_INT_FORWARD(METHOD)						\
  template <typename... Args>						\
  auto METHOD(Transaction &t, Args&&... args) {				\
    return with_trans_intr(						\
      t,								\
    [this](auto&&... args) {						\
      return this->_##METHOD(args...);					\
    },									\
    std::forward<Args>(args)...);					\
  }

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  using base_ertr = Cache::get_extent_ertr;
  using base_iertr = trans_iertr<Cache::get_extent_ertr>;

  using mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(
    Transaction &t
  ) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not resolve until all pins have resolved (set_paddr called)
   */
  using get_mappings_ertr = base_ertr;
  using get_mappings_iertr = trans_iertr<base_ertr>;
  using get_mappings_ret = get_mappings_iertr::future<lba_pin_list_t>;
  virtual get_mappings_ret _get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) = 0;

  /**
   * Fetches mappings for a list of laddr_t in range [offset, offset + len)
   *
   * Future will not resolve until all pins have resolved (set_paddr called)
   */
  virtual get_mappings_ret _get_mappings(
    Transaction &t,
    laddr_list_t &&extent_lisk) = 0;
  LBA_INT_FORWARD(get_mappings)

  /**
   * Fetches the mapping for laddr_t
   *
   * Future will not resolve until the pin has resolved (set_paddr called)
   */
  using get_mapping_ertr = base_ertr::extend<
    crimson::ct_error::enoent>;
  using get_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using get_mapping_ret = get_mapping_iertr::future<LBAPinRef>;
  virtual get_mapping_ret _get_mapping(
    Transaction &t,
    laddr_t offset) = 0;
  LBA_INT_FORWARD(get_mapping)

  /**
   * Finds unmapped laddr extent of len len
   */
  using find_hole_ertr = base_ertr;
  using find_hole_iertr = base_iertr;
  using find_hole_ret = find_hole_iertr::future<
    std::pair<laddr_t, extent_len_t>
    >;
  virtual find_hole_ret _find_hole(
    Transaction &t,
    laddr_t hint,
    extent_len_t) = 0;
  LBA_INT_FORWARD(find_hole)

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAPin.
   */
  using alloc_extent_ertr = base_ertr;
  using alloc_extent_iertr = base_iertr;
  using alloc_extent_ret = alloc_extent_iertr::future<LBAPinRef>;
  virtual alloc_extent_ret _alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) = 0;
  LBA_INT_FORWARD(alloc_extent)

  /**
   * Creates a new absolute mapping.
   *
   * off~len must be unreferenced
   */
  using set_extent_iertr = base_iertr::extend<
    crimson::ct_error::invarg>;
  using set_extent_ret = set_extent_iertr::future<LBAPinRef>;
  virtual set_extent_ret _set_extent(
    Transaction &t,
    laddr_t off, extent_len_t len, paddr_t addr) = 0;
  LBA_INT_FORWARD(set_extent)


  struct ref_update_result_t {
    unsigned refcount = 0;
    paddr_t addr;
    extent_len_t length = 0;
  };
  using ref_ertr = base_ertr::extend<
    crimson::ct_error::enoent>;
  using ref_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using ref_ret = ref_iertr::future<ref_update_result_t>;

  /**
   * Decrements ref count on extent
   *
   * @return returns resulting refcount
   */
  virtual ref_ret _decref_extent(
    Transaction &t,
    laddr_t addr) = 0;
  LBA_INT_FORWARD(decref_extent)

  /**
   * Increments ref count on extent
   *
   * @return returns resulting refcount
   */
  virtual ref_ret _incref_extent(
    Transaction &t,
    laddr_t addr) = 0;
  LBA_INT_FORWARD(incref_extent)

  using complete_transaction_iertr = base_iertr;
  using complete_transaction_ret = complete_transaction_iertr::future<>;
  virtual complete_transaction_ret _complete_transaction(
    Transaction &t) = 0;
  LBA_INT_FORWARD(complete_transaction)

  /**
   * Should be called after replay on each cached extent.
   * Implementation must initialize the LBAPin on any
   * LogicalCachedExtent's and may also read in any dependent
   * structures, etc.
   */
  using init_cached_extent_iertr = base_iertr;
  using init_cached_extent_ret = init_cached_extent_iertr::future<>;
  virtual init_cached_extent_ret _init_cached_extent(
    Transaction &t,
    CachedExtentRef e) = 0;
  LBA_INT_FORWARD(init_cached_extent)

  /**
   * Calls f for each mapping in [begin, end)
   */
  using scan_mappings_iertr = base_iertr;
  using scan_mappings_ret = scan_mappings_iertr::future<>;
  using scan_mappings_func_t = std::function<
    void(laddr_t, paddr_t, extent_len_t)>;
  virtual scan_mappings_ret _scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) = 0;
  LBA_INT_FORWARD(scan_mappings)

  /**
   * Calls f for each mapped space usage in [begin, end)
   */
  using scan_mapped_space_iertr = base_iertr::extend_ertr<
    SegmentManager::read_ertr>;
  using scan_mapped_space_ret = scan_mapped_space_iertr::future<>;
  using scan_mapped_space_func_t = std::function<
    void(paddr_t, extent_len_t)>;
  virtual scan_mapped_space_ret _scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) = 0;
  LBA_INT_FORWARD(scan_mapped_space)

  /**
   * rewrite_extent
   *
   * rewrite extent into passed transaction
   */
  using rewrite_extent_iertr = base_iertr;
  using rewrite_extent_ret = rewrite_extent_iertr::future<>;
  virtual rewrite_extent_ret _rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) = 0;
  LBA_INT_FORWARD(rewrite_extent)

  /**
   * get_physical_extent_if_live
   *
   * Returns extent at addr/laddr if still live (if laddr
   * still points at addr).  Extent must be an internal, physical
   * extent.
   *
   * Returns a null CachedExtentRef if extent is not live.
   */
  using get_physical_extent_if_live_iertr = base_iertr;
  using get_physical_extent_if_live_ret =
    get_physical_extent_if_live_iertr::future<CachedExtentRef>;
  virtual get_physical_extent_if_live_ret _get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    segment_off_t len) = 0;
  LBA_INT_FORWARD(get_physical_extent_if_live)

  virtual void add_pin(LBAPin &pin) = 0;

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
