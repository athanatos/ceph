// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "fwd.h"
#include "super.h"
#include "node_extent_mutable.h"
#include "node_types.h"

/**
 * node_extent_manager.h
 *
 * Contains general interfaces for different backends (Dummy and Seastore).
 */

namespace crimson::os::seastore::onode {

using crimson::os::seastore::LogicalCachedExtent;
class NodeExtent : public LogicalCachedExtent {
 public:
  virtual ~NodeExtent() = default;
  std::pair<node_type_t, field_type_t> get_types() const;
  const char* get_read() const {
    return get_bptr().c_str();
  }
  NodeExtentMutable get_mutable() {
    assert(is_pending());
    return do_get_mutable();
  }

  virtual DeltaRecorder* get_recorder() const = 0;
  virtual NodeExtentRef mutate(context_t, DeltaRecorderURef&&) = 0;

 protected:
  template <typename... T>
  NodeExtent(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  NodeExtentMutable do_get_mutable() {
    return NodeExtentMutable(get_bptr().c_str(), get_length());
  }

  /**
   * Abstracted interfaces to implement:
   * - CacheExtent::duplicate_for_write() -> CachedExtentRef
   * - CacheExtent::get_type() -> extent_types_t
   * - CacheExtent::get_delta() -> ceph::bufferlist
   * - LogicalCachedExtent::apply_delta(const ceph::bufferlist) -> void
   */
};

using crimson::os::seastore::TransactionManager;
class NodeExtentManager {
  using base_ertr = eagain_ertr::extend<
    crimson::ct_error::input_output_error>;

 public:
  virtual ~NodeExtentManager() = default;

  virtual bool is_read_isolated() const = 0;

  using read_ertr = base_ertr::extend<
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<NodeExtentRef> read_extent(
      Transaction&, laddr_t) = 0;

  using alloc_ertr = base_ertr;
  virtual alloc_ertr::future<NodeExtentRef> alloc_extent(
      Transaction&, extent_len_t) = 0;

  using retire_ertr = base_ertr::extend<
    crimson::ct_error::enoent>;
  virtual retire_ertr::future<> retire_extent(
      Transaction&, NodeExtentRef) = 0;

  using getsuper_ertr = base_ertr;
  virtual getsuper_ertr::future<Super::URef> get_super(
      Transaction&, RootNodeTracker&) = 0;

  virtual std::ostream& print(std::ostream& os) const = 0;

  static NodeExtentManagerURef create_dummy(bool is_sync);
  static NodeExtentManagerURef create_seastore(
      InterruptedTransactionManager tm, laddr_t min_laddr = L_ADDR_MIN, double p_eagain = 0.0);
};
inline std::ostream& operator<<(std::ostream& os, const NodeExtentManager& nm) {
  return nm.print(os);
}

}
