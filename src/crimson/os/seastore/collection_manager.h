// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "osd/osd_types.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

#define COLL_INIT_BLOCK 4096

namespace crimson::os::seastore {

/**
  * coll_root_t
  *
  * Information for locating CollectionManager information, addr should be
  * embedded into the TransactionManager root.
  */
  class coll_root_t {
    laddr_t coll_root_laddr;

    enum state_t : uint8_t {
      CLEAN = 0,   /// No pending mutations
      MUTATED = 1, /// coll_root_laddr state must be written back to persistence
      NONE = 0xFF  /// Not yet mounted, should not be exposed to user
    } state = NONE;

  public:
    coll_root_t(laddr_t laddr)
    : coll_root_laddr(laddr) {}

    bool must_update_location() const {
      return state == MUTATED;
    }

    laddr_t get_location() const {
      return coll_root_laddr;
    }
    void set_location(laddr_t laddr) {
      coll_root_laddr = laddr;
    }
    state_t get_status() const{
      return state;
    }
    void set_status(state_t s) {
      state = s;
    }
  };

  struct coll_info_t {
    unsigned split_bits;

    coll_info_t(unsigned bits)
    : split_bits(bits) {}
  };

/// Interface for maintaining set of collections
class CollectionManager {
public:
  using base_ertr = TransactionManager::read_extent_ertr;
    /// Initialize collection manager instance for an empty store
  using mkfs_ertr = TransactionManager::alloc_extent_ertr;
  using mkfs_ret = mkfs_ertr::future<coll_root_t>;
  virtual mkfs_ret mkfs(
    Transaction &t,
    unsigned block_size) = 0;

  /// Create collection
  using create_ertr = base_ertr;
  using create_ret = create_ertr::future<bool>;
  virtual create_ret create(
    coll_root_t &root,
    Transaction &t,
    coll_t cid,
    coll_info_t info
  ) = 0;

  /// List collections with info
  using list_ertr = base_ertr;
  using list_ret_bare = std::vector<std::pair<coll_t, coll_info_t>>;
  using list_ret = list_ertr::future<list_ret_bare>;
  virtual list_ret list(
    const coll_root_t &root,
    Transaction &t) = 0;

  /// Remove cid
  using remove_ertr = base_ertr;
  using remove_ret = remove_ertr::future<bool>;
  virtual remove_ret remove(
    const coll_root_t &coll_root,
    Transaction &t,
    coll_t cid) = 0;

  /// Update info for cid
  using update_ertr = base_ertr;
  using update_ret = base_ertr::future<bool>;
  virtual update_ret update(
    coll_root_t &coll_root,
    Transaction &t,
    coll_t cid,
    coll_info_t info
  ) = 0;

  virtual ~CollectionManager() {}
};
using CollectionManagerRef = std::unique_ptr<CollectionManager>;

namespace collection_manager {
/* creat CollectionMapManager for Collection  */
CollectionManagerRef create_coll_manager(
  TransactionManager &trans_manager);

}

}
