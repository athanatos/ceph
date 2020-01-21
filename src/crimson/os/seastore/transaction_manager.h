// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>

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
  
  const paddr_t start;

public:
  Transaction(paddr_t start);
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
    return std::make_unique<Transaction>(paddr_t{0,0});
  }

  using read_ertr = crimson::errorator <
    crimson::ct_error::input_output_error
    >;
  /**
   * Add journaled representation of overwrite of physical offsets
   * [addr, addr + bl.size()).
   *
   * As read_ertr indicates, may perform reads.
   *
   * @param trans [in,out] current transaction
   * @param paddr [in]     aligned physical block address
   * @param laddr [in]     aligned logical block address or type tag for
   *                       non-logical blocks
   * @param delta [in]     logical mutation -- encoding of mutation
   *                       to block
   * @param bytes [in]     vector of offset, bufferlist pairs representing
   *                       physical mutation to blocks
   * @return future for completion
   */
  read_ertr::future<> add_delta(
    TransactionRef &trans,
    paddr_t paddr,
    laddr_t laddr,
    ceph::bufferlist delta,
    ceph::bufferlist bl);

  /**
   * Adds a new block containing bl.
   *
   * @param trans [in,out] current transaction
   * @param laddr [in] laddr of block or type code for non-logical
   * @param bl    [in] contents of new bloc, must be page aligned and have
   *                   aligned length.
   *
   * @return 
   */
  paddr_t add_block(
    TransactionRef &trans,
    laddr_t laddr,
    ceph::bufferlist bl);

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
