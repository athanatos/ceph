// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

std::tuple<ExtentSet, extent_list_t, extent_list_t>
Cache::get_reserve_extents(
  laddr_t offset,
  loff_t length)
{
  return std::make_pair(
    ExtentSet(),
    extent_list_t(),
    extent_list_t()
  );
}

void Cache::present_reserved_extents(
  ExtentSet &extents)
{
}

Cache::wait_extents_ertr::future<ExtentSet>
Cache::await_pending(const extent_list_t &pending)
{
  return wait_extents_ertr::now();
}

CachedExtentRef Cache::get_extent_buffer(
  laddr_t offset,
  loff_t length)
{
  return CachedExtentRef(); // TODO
}

}
