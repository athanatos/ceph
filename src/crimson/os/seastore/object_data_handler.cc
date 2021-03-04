// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/extentmap_manager/btree/btree_extentmap_manager.h"
#include "crimson/os/seastore/object_data_handler.h"

namespace crimson::os::seastore {

template <bool create, typename F>
auto with_extmap(
  TransactionManager &tm,
  Transaction &t,
  Onode &onode,
  F &&f) {
  return seastar::do_with(
    extentmap_manager::BtreeExtentMapManager(tm),
    onode.get_layout().extmap_root.get(),
    std::forward<F>(f),
    [&](auto &emanager, auto &extmap_root, auto &f) {
      auto may_create = ObjectDataHandler::base_ertr::now();
      if (create && extmap_root.is_null()) {
	may_create = emanager.initialize_extmap(t
	).safe_then([&](auto nroot) {
	  extmap_root = nroot;
	  extmap_root.mutated = true;
	});
      }
      return may_create.safe_then([&] {
	return std::invoke(f, emanager, extmap_root);
      }).safe_then([&] {
	if (extmap_root.must_update()) {
	  onode.get_mutable_layout(t).extmap_root.update(extmap_root);
	}
	return seastar::now();
      });
    });
}

ObjectDataHandler::write_ret ObjectDataHandler::write(
  TransactionManager &tm,
  Transaction &t,
  Onode &onode,
  objaddr_t offset,
  const bufferlist &bl)
{
  return with_extmap<true>(
    tm, t, onode,
    [offset, &bl](auto &manager, auto &root) {
      return seastar::now();
    });
}

ObjectDataHandler::read_ret ObjectDataHandler::read(
  TransactionManager &tm,
  Transaction &t,
  Onode &onode,
  objaddr_t offset,
  extent_len_t len)
{
  return seastar::do_with(
    bufferlist(),
    [&, offset, len](auto &ret) {
      return with_extmap<false>(
	tm, t, onode,
	[&, offset, len](auto &manager, auto &root) {
	  return seastar::now();
	}).safe_then([&ret] {
	  return std::move(ret);
	});
    });
}
  
ObjectDataHandler::truncate_ret ObjectDataHandler::truncate(
  TransactionManager &tm,
  Transaction &t,
  Onode &onode,
  objaddr_t offset)
{
  return with_extmap<true>(
    tm, t, onode,
    [offset](auto &manager, auto &root) {
      return seastar::now();
    });
}

}
