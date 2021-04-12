// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include "include/buffer.h"

#include "test/crimson/seastore/test_block.h" // TODO

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

class ObjectDataHandler {
public:
  struct config_t {
    extent_len_t default_object_reservation = 4<<20; /* 4MB */
  } config;


  using base_ertr = TransactionManager::base_ertr;

  struct context_t {
    TransactionManager &tm;
    Transaction &t;
    Onode &onode;
  };

  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<>;
  write_ret write(
    context_t ctx,
    objaddr_t offset,
    const bufferlist &bl);

  using read_ertr = base_ertr;
  using read_ret = read_ertr::future<bufferlist>;
  read_ret read(
    context_t ctx,
    objaddr_t offset,
    extent_len_t len);
  
  using truncate_ertr = base_ertr;
  using truncate_ret = truncate_ertr::future<>;
  truncate_ret truncate(
    context_t ctx,
    objaddr_t offset);

  using clear_ertr = base_ertr;
  using clear_ret = clear_ertr::future<>;
  clear_ret clear(context_t ctx);

private:
  extent_len_t get_reservation_size(
    extent_len_t size) {
    return size + (
      config.default_object_reservation -
      (size % config.default_object_reservation));
  }

  auto read_pin(
    context_t ctx,
    LBAPinRef pin) {
    return ctx.tm.pin_to_extent<TestBlock>(
      ctx.t,
      std::move(pin)
    ).handle_error(
      write_ertr::pass_further{},
      crimson::ct_error::assert_all{ "read_pin: invalid error" }
    );
  }

  write_ret overwrite(
    context_t ctx,
    laddr_t offset,
    bufferlist &&bl,
    lba_pin_list_t &&pins);

  write_ret expand_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);

  clear_ret shrink_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);
};

}
