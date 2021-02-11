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
    STABLE,
    MUTATED,
    DELETED
  } status;

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

  const onode_layout_t &get_layout() const final {
    return *read_payload<onode_layout_t>();
  }

  onode_layout_t &get_mutable_layout(Transaction &t) final {
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    status = status_t::MUTATED;
    return *reinterpret_cast<onode_layout_t*>(p.first.get_write());
  };

  void populate_recorder(Transaction &t) {
    auto p = prepare_mutate_payload<
      onode_layout_t,
      Recorder>(t);
    status = status_t::STABLE;
    // TODO: fill in recorder
  }

  ~FLTreeOnode() final {}
};

using OnodeTree = Btree<FLTreeOnode>;

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
  OnodeTree tree;

public:
  FLTreeOnodeManager(TransactionManager &tm) :
    tree(std::make_unique<SeastoreNodeExtentManager>(
	   tm, laddr_t{})) {}

  mkfs_ret mkfs(Transaction &t) {
    return tree.mkfs(t
    ).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in FLTreeOnodeManager::mkfs"
      }
    );
  }

  get_onode_ret get_onode(
    Transaction &trans,
    const ghobject_t &hoid) final {
    return tree.find(
      trans, hoid
    ).safe_then([this, &trans, &hoid](auto cursor)
		-> get_onode_ret {
      if (cursor == tree.end()) {
	return crimson::ct_error::enoent::make();
      }
      auto val = OnodeRef(new FLTreeOnode(cursor.value()));
      return seastar::make_ready_future<OnodeRef>(
	val
      );
    }).handle_error(
      get_onode_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in FLTreeOnodeManager::get_onode"
      }
    );
  }

  
  get_or_create_onode_ret get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) final {
    return tree.insert(
      trans, hoid,
      OnodeTree::tree_value_config_t{sizeof(onode_layout_t)}
    ).safe_then([this, &trans, &hoid](auto p)
		-> get_or_create_onode_ret {
      auto [cursor, created] = std::move(p);
      auto val = OnodeRef(new FLTreeOnode(cursor.value()));
      if (created) {
	val->get_mutable_layout(trans) = onode_layout_t{};
      }
      return seastar::make_ready_future<OnodeRef>(
	val
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
    return seastar::do_with(
      std::vector<OnodeRef>(),
      [this, &hoids, &trans](auto &ret) {
	ret.reserve(hoids.size());
	return crimson::do_for_each(
	  hoids,
	  [this, &trans, &ret](auto &hoid) {
	    return get_or_create_onode(trans, hoid
	    ).safe_then([this, &ret](auto &&onoderef) {
	      ret.push_back(std::move(onoderef));
	    });
	  }).safe_then([&ret] {
	    return std::move(ret);
	  });
      });
  }

  write_dirty_ret write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) final {
    return crimson::do_for_each(
      onodes,
      [this, &trans](auto &onode) -> OnodeTree::btree_future<> {
	auto &flonode = static_cast<FLTreeOnode&>(*onode);
	switch (flonode.status) {
	case FLTreeOnode::status_t::MUTATED: {
	  flonode.populate_recorder(trans);
	  return seastar::now();
	}
	case FLTreeOnode::status_t::DELETED: {
	  return tree.erase(trans, flonode).safe_then([](auto) {});
	}
	case FLTreeOnode::status_t::STABLE: {
	  return seastar::now();
	}
	default:
	  __builtin_unreachable();
	}
      }).handle_error(
	write_dirty_ertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Invalid error in FLTreeOnodeManager::get_or_create_onode"
	}
      );
  }

  ~FLTreeOnodeManager();
};

}
