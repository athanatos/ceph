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

struct ObjectDataBlock : crimson::os::seastore::LogicalCachedExtent {
  using Ref = TCachedExtentRef<ObjectDataBlock>;

  ObjectDataBlock(ceph::bufferptr &&ptr)
    : LogicalCachedExtent(std::move(ptr)) {}
  ObjectDataBlock(const ObjectDataBlock &other)
    : LogicalCachedExtent(other) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new ObjectDataBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::OBJECT_DATA_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    /* Currently, we always allocate fresh ObjectDataBlock's rather than
     * mutating existing ones. */
    ceph_assert(0 == "Should be impossible");
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    // See get_delta()
    ceph_assert(0 == "Should be impossible");
  }
};
using ObjectDataBlockRef = TCachedExtentRef<ObjectDataBlock>;

struct ObjectDataBlockPhysical : crimson::os::seastore::CachedExtent{
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<ObjectDataBlockPhysical>;

  std::vector<test_block_delta_t> delta = {};

  ObjectDataBlockPhysical(ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)) {}
  ObjectDataBlockPhysical(const ObjectDataBlock &other)
    : CachedExtent(other) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new ObjectDataBlockPhysical(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK_PHYSICAL;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void set_contents(char c, uint16_t offset, uint16_t len) {
    ::memset(get_bptr().c_str() + offset, c, len);
  }

  void set_contents(char c) {
    set_contents(c, 0, get_length());
  }

  ceph::bufferlist get_delta() final { return ceph::bufferlist(); }

  void apply_delta_and_adjust_crc(paddr_t, const ceph::bufferlist &bl) final {}
};
using ObjectDataBlockPhysicalRef = TCachedExtentRef<ObjectDataBlockPhysical>;

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
    return ctx.tm.pin_to_extent<ObjectDataBlock>(
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

  write_ret prepare_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);

  clear_ret trim_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);
};

}
