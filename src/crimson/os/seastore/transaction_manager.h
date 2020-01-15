// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class Transaction {
public:
};
using TransactionRef = std::unique_ptr<Transaction>;

class TransactionManager {
public:
  TransactionRef create_transaction() {
    return std::make_unique<Transaction>();
  }

  using submit_transaction_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  submit_transaction_ertr::future<> submit_transaction() {
    return submit_transaction_ertr::now();
  }
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
