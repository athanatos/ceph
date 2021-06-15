// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include "include/buffer.h"

#include "test/crimson/seastore/test_block.h" // TODO

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/transaction_manager.h"
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

class ObjectDataHandler {
public:
  using base_ertr = TransactionManager::base_ertr;

  struct context_t {
    InterruptedTransactionManager tm;
    Transaction &t;
    Onode &onode;
  };

  /// Writes bl to [offset, offset + bl.length())
  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<>;
  write_ret write(
    context_t ctx,
    objaddr_t offset,
    const bufferlist &bl);

  /// Reads data in [offset, offset + len)
  using read_ertr = base_ertr;
  using read_ret = read_ertr::future<bufferlist>;
  read_ret read(
    context_t ctx,
    objaddr_t offset,
    extent_len_t len);

  /// Clears data past offset
  using truncate_ertr = base_ertr;
  using truncate_ret = truncate_ertr::future<>;
  truncate_ret truncate(
    context_t ctx,
    objaddr_t offset);

  /// Clears data and reservation
  using clear_ertr = base_ertr;
  using clear_ret = clear_ertr::future<>;
  clear_ret clear(context_t ctx);

private:
  /// Updates region [_offset, _offset + bl.length) to bl
  write_ret overwrite(
    context_t ctx,        ///< [in] ctx
    laddr_t offset,       ///< [in] write offset
    bufferlist &&bl,      ///< [in] buffer to write
    lba_pin_list_t &&pins ///< [in] set of pins overlapping above region
  );

  /// Ensures object_data reserved region is prepared
  write_ret prepare_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);

  /// Trims data past size
  clear_ret trim_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);
};

}
