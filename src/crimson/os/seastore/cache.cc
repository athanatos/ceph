// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

bool Cache::try_begin_commit(Transaction &t)
{
  // First, validate read set
  for (auto &i: t.read_set) {
    if (i->state == CachedExtent::extent_state_t::INVALID)
      return false;
  }

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  for (auto &i: t.retired_set) {
    ceph_assert(i->state == CachedExtent::extent_state_t::WRITTEN);
    i->state = CachedExtent::extent_state_t::INVALID;
    extents.erase(*i);
  }

  for (auto &i: t.fresh_block_list) {
    // Sanity check
    auto [a, b] = extents.get_overlap(i->get_paddr(), i->get_length());
    ceph_assert(a == b);
    extents.insert(*i);
    i->set_io_wait();
  }

  //for (auto &i: t.
  return true;
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_record_location,
  paddr_t final_block_start)
{
}

Cache::replay_delta_ret
Cache::replay_delta(const delta_info_t &delta)
{
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}


}
