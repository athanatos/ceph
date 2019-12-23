// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

Cache::get_extent_ret Cache::get_extent(
  Transaction &t,
  paddr_t offset,
  segment_off_t length)
{
  return get_extent_ertr::make_ready_future<CachedExtentRef>();
}

std::tuple<ExtentSet, extent_list_t, extent_list_t>
Cache::get_reserve_extents(const extent_list_t &extents)
{
  return std::make_tuple(
    ExtentSet(),
    extent_list_t(),
    extent_list_t()
  );
}

void Cache::present_reserved_extents(
  ExtentSet &extents)
{
}

Cache::await_pending_fut Cache::await_pending(const extent_list_t &pending)
{
  return await_pending_fut(await_pending_ertr::ready_future_marker{});
}

CachedExtentRef Cache::get_extent_buffer(
  laddr_t offset,
  loff_t length)
{
  return CachedExtentRef(); // TODO
}

void Cache::update_extents(
  ExtentSet &extents,
  const std::list<std::pair<paddr_t, segment_off_t>> &to_release)
{
}

Cache::replay_delta_ret
Cache::replay_delta(const delta_info_t &delta)
{
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}


}
