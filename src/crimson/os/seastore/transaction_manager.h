// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {
class SegmentManager;

class Journal;

class Transaction {
  friend class TransactionManager;

  std::vector<
    std::pair<
      segment_off_t,
      std::function<block_info_t(paddr_t)>
      >
    > blocks;

  std::vector<std::function<delta_info_t(paddr_t)>> deltas;
public:
  segment_off_t block_offset = 0;

  /**
   * Add journaled representation of overwrite of physical offsets
   * [addr, addr + bl.size()).
   *
   * As read_ertr indicates, may perform reads.
   *
   * @param trans [in,out] current transaction
   * @param type  [in]     type of delta
   * @param laddr [in]     aligned logical block address -- null 
   *                       ff type != LBA_BLOCK
   * @param delta [in]     logical mutation -- encoding of mutation
   *                       to block
   * @return future for completion
   */
  template <typename F>
  void add_delta(F &&f) {
    deltas.emplace_back(std::move(f));
  }

  /**
   * Adds a new block containing bl.
   *
   * @param bl    [in] contents of new bloc, must be page aligned and have
   *                   aligned length.
   * @return 
   */
  template <typename F>
  segment_off_t add_block(
    segment_off_t length,
    F &&f) {
    blocks.emplace_back(std::make_pair(length, std::move(f)));
    auto current = block_offset;
    block_offset += length;
    return block_offset;
  }
    

  
};
using TransactionRef = std::unique_ptr<Transaction>;

class TransactionManager {
  std::unique_ptr<Journal> journal;

public:
  TransactionManager(SegmentManager &segment_manager);

  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> init();

  TransactionRef create_transaction() {
    return std::make_unique<Transaction>();
  }

  /**
   * Atomically submits transaction to persistence and cache
   *
   * @param 
   */
  using submit_transaction_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  submit_transaction_ertr::future<> submit_transaction() {
    return submit_transaction_ertr::now();
  }

  ~TransactionManager();
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
