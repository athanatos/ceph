// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/value.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"

namespace crimson::os::seastore::onode {

struct FLTreeOnode : Onode, Value {
  static constexpr value_magic_t HEADER_MAGIC = value_magic_t::ONODE;

  enum class status_t {
    NEW,
    STABLE,
    MUTATED,
    DELETED
  } status;

  // TODO: can be a union
  using cursor_t = Btree<FLTreeOnode>::Cursor;
  std::optional<cursor_t> cursor;
  std::optional<onode_layout_t> mutated;

  FLTreeOnode(FLTreeOnode&&) = default;
  FLTreeOnode& operator=(FLTreeOnode&&) = delete;

  FLTreeOnode(const FLTreeOnode&) = default;
  FLTreeOnode& operator=(const FLTreeOnode&) = delete;

  template <typename... T>
  FLTreeOnode(T&&... args) : Value(std::forward<T>(args)...) {}

  struct Recorder : public ValueDeltaRecorder {
    Recorder(bufferlist &bl) : ValueDeltaRecorder(bl) {}
    
    value_magic_t get_header_magic() const final {
      return value_magic_t::ONODE;
    }
    
    void apply_value_delta(
      ceph::bufferlist::const_iterator&,
      NodeExtentMutable&,
      laddr_t) final {
      // TODO
    }
  };

  onode_layout_t &get_layout() const final {
    return *(static_cast<onode_layout_t*>(nullptr));
  }
#if 0
  void mark_mutable() final {
    // TODO
  }
#endif
};

using OnodeTree = Btree<FLTreeOnode>;

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
  OnodeTree tree;

public:
  FLTreeOnodeManager(TransactionManager &tm) :
    tree(std::make_unique<SeastoreNodeExtentManager>(
	   tm, laddr_t{})) {}
    
  
  get_or_create_onode_ret get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) final {
    return tree.find(trans, hoid
    ).safe_then([this, &trans, &hoid](auto cursor)
		-> get_or_create_onode_ret {
      
      return seastar::make_ready_future<OnodeRef>(
	OnodeRef(new FLTreeOnode(cursor.value()))
      );
    }).handle_error(
      get_or_create_onode_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in FLTreeOnodeManager::get_or_create_onode"
      }
    );
  }
  get_or_create_onodes_ret get_or_create_onodes(
    Transaction &trans,
    const std::vector<ghobject_t> &hoids) final {
    return seastar::make_ready_future<std::vector<OnodeRef>>();
  }

  write_dirty_ret write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) final {
    return seastar::now();
  }

  ~FLTreeOnodeManager();
};

}
