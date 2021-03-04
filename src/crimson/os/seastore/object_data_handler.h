// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include "include/buffer.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

class ObjectDataHandler {
public:
  using base_ertr = TransactionManager::base_ertr;

  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<>;
  static write_ret write(
    TransactionManager &tm,
    Transaction &t,
    Onode &onode,
    objaddr_t offset,
    const bufferlist &bl);

  using read_ertr = base_ertr;
  using read_ret = read_ertr::future<bufferlist>;
  static read_ret read(
    TransactionManager &tm,
    Transaction &t,
    Onode &onode,
    objaddr_t offset,
    extent_len_t len);
  
  using truncate_ertr = base_ertr;
  using truncate_ret = truncate_ertr::future<>;
  static truncate_ret truncate(
    TransactionManager &tm,
    Transaction &t,
    Onode &onode,
    objaddr_t offset);
};

}
