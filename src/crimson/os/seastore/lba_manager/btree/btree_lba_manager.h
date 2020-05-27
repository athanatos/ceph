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

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"

namespace crimson::os::seastore::lba_manager::btree {

/**
 * BtreeLBAManager
 *
 * Uses a wandering btree to track two things:
 * 1) lba state including laddr_t -> paddr_t mapping
 * 2) reverse paddr_t -> laddr_t mapping for gc (TODO)
 *
 * Generally, any transaction will involve
 * 1) deltas against lba tree nodes
 * 2) new lba tree nodes
 *    - Note, there must necessarily be a delta linking
 *      these new nodes into the tree -- might be a
 *      bootstrap_state_t delta if new root
 *
 * get_mappings, alloc_extent_*, etc populate a Transaction
 * which then gets submitted
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(
    SegmentManager &segment_manager,
    Cache &cache);

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&list) final;

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) final;

  set_extent_ret set_extent(
    Transaction &t,
    laddr_t off, extent_len_t len, paddr_t addr) final;

  decref_extent_ret decref_extent(
    Transaction &t,
    laddr_t addr) final;
  incref_extent_ret incref_extent(
    Transaction &t,
    laddr_t addr) final;

  submit_lba_transaction_ret submit_lba_transaction(
    Transaction &t) final;

  TransactionRef create_transaction() final {
    auto t = new Transaction;
    return TransactionRef(t);
  }
private:
  SegmentManager &segment_manager;
  Cache &cache;

  /**
   * get_root
   *
   * Get a reference to the root LBANode.
   */
  using get_root_ertr = Cache::get_extent_ertr;
  using get_root_ret = get_root_ertr::future<LBANodeRef>;
  get_root_ret get_root(Transaction &);

  /**
   * insert_mapping
   *
   * Insert a lba mapping into the tree
   */
  using insert_mapping_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using insert_mapping_ret = insert_mapping_ertr::future<LBAPinRef>;
  insert_mapping_ret insert_mapping(
    Transaction &t,   ///< [in,out] transaction
    LBANodeRef root,  ///< [in] root node
    laddr_t laddr,    ///< [in] logical addr to insert
    lba_map_val_t val ///< [in] mapping to insert
  );
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}
