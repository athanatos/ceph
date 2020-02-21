// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

std::tuple<pextent_list_t, paddr_list_t, paddr_list_t>
Cache::get_reserve_extents(
  paddr_list_t &extents)
{
  return std::make_tuple(
    pextent_list_t(),
    paddr_list_t(),
    paddr_list_t()
  );
}

void Cache::present_reserved_extents(
  paddr_list_t &extents)
{
}

Cache::await_pending_fut Cache::await_pending(const paddr_list_t &pending)
{
  return await_pending_ertr::make_ready_future<pextent_list_t>();
}

using read_extent_ertr = SegmentManager::read_ertr;
using read_extent_ret = read_extent_ertr::future<pextent_list_t>;
Cache::read_extent_ret Cache::read_extents(
  paddr_list_t &extents)
{
#if 0
  return seastar::do_with(
    std::make_tuple(std::move(need), ExtentSet()),
    [this, &t](auto &tup) -> read_extent_ret {
      auto &[need, read] = tup;
      return crimson::do_for_each(
	need.begin(),
	need.end(),
	[this, &read, &t](auto &iter) {
	  auto &[offset, length] = iter;
	  return lba_manager->get_mappings(
	    offset, length, t).safe_then(
	      [this, &read, &t](auto &&pin_refs) {
		return seastar::do_with(
		  std::move(pin_refs),
		  [this, &read](auto &lba_pins) {
		    return crimson::do_for_each(
		      lba_pins.begin(),
		      lba_pins.end(),
		      [this, &read](auto &pin) {
			// TODO: invert buffer control so that we pass the buffer
			// here into segment_manager to avoid a copy
			return segment_manager.read(
			  pin->get_paddr(),
			  pin->get_length()
			).safe_then([this, &read, &pin](auto bl) mutable {
			  auto eref = cache.get_extent_buffer(
			    pin->get_laddr(),
			    pin->get_length());
			  eref->set_pin(std::move(pin));
			  eref->copy_in(bl, pin->get_laddr(), pin->get_length());
			  read.insert(eref);
			  return read_extent_ertr::now();
			});
		      });
		  });
	      });
	}).safe_then([this, &read]() mutable {
	  // offer all to transaction
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(read));
	});
    }).safe_then([this, &t, all=std::move(all), pending=std::move(pending)](
		   auto &&read) {
      return cache.await_pending(pending).safe_then(
	[this, &t, all=std::move(all), read=std::move(read)](
	  auto &&pending_extents) mutable {
	  t.add_to_read_set(pending_extents);
	  t.add_to_read_set(read);
	  all.merge(std::move(pending_extents));
	  all.merge(std::move(read));
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(all));
	});
    });
#endif
  return read_extent_ertr::make_ready_future<pextent_list_t>();
}
  
Cache::replay_delta_ret
Cache::replay_delta(const delta_info_t &delta)
{
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}


}
