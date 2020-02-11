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
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"

#include "crimson/os/seastore/lba_manager/btree/btree_node.h"

namespace crimson::os::seastore::lba_manager::btree {

/**
 * BtreeLBAManager
 *
 * Uses a wandering btree to track two things:
 * 1) lba state including laddr_t -> paddr_t mapping
 * 2) reverse paddr_t -> laddr_t mapping for gc
 *
 * Generally, any transaction will involve
 * 1) deltas against lba tree nodes
 * 2) new lba tree nodes
 *    - Note, there must necessarily be a delta linking
 *      these new nodes into the tree -- might be a
 *      bootstrap_state_t delta if new root
 * 3) the record itself acts as an implicit delta against
 *    the unwritten_segment_update tree
 *
 * get_mappings, alloc_extent_*, etc populate an Transaction
 * which then gets submitted
 */
class BtreeLBAManager : public LBAManager {
  SegmentManager &segment_manager;
  Cache &cache;

  /* Soft state, maintained via special deltas,
   * never actually written as a block
   */
  struct bootstrap_state_t {
    depth_t lba_depth;
    depth_t segment_depth;
    paddr_t lba_root_addr;
    paddr_t segment_root;
  } bootstrap_state;
  
public:
  BtreeLBAManager(
    SegmentManager &segment_manager,
    Cache &cache);

  get_mapping_ret get_mappings(
    laddr_t offset, loff_t length,
    Transaction &t) final;

  alloc_extent_relative_ret alloc_extent_relative(
    laddr_t hint,
    loff_t len,
    segment_off_t offset,
    Transaction &t) final;

  set_extent_ret set_extent(
    laddr_t off, loff_t len, paddr_t addr,
    Transaction &t) final;

  set_extent_relative_ret set_extent_relative(
    laddr_t off, loff_t len, segment_off_t record_offset,
    Transaction &t) final;

  bool decref_extent(LBAPinRef &ref, Transaction &t) final;
  void incref_extent(LBAPinRef &ref, Transaction &t) final;

  submit_lba_transaction_ret submit_lba_transaction(
    Transaction &t) final;

};
  
}
