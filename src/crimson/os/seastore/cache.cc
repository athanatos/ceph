// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

CachedExtentRef Cache::duplicate_for_write(
  Transaction &t,
  CachedExtentRef i) {
  if (i->is_pending())
    return i;

  auto ret = i->duplicate_for_write();
  ret->version++;
  ret->state = CachedExtent::extent_state_t::PENDING_DELTA;
  return ret;
}

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

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    extents.insert(*i);
    i->set_io_wait();
  }

  t.write_set.clear();
  t.read_set.clear();
  return true;
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_block_start)
{
  paddr_t cur = final_block_start;
  for (auto &i: t.fresh_block_list) {
    i->set_paddr(cur);
    cur.offset += i->get_length();
    i->state = CachedExtent::extent_state_t::WRITTEN;
    i->on_written(final_block_start);
    extents.insert(*i);
  }

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    i->state = CachedExtent::extent_state_t::WRITTEN;
    i->on_written(final_block_start);
  }

  for (auto &i: t.fresh_block_list) {
    i->complete_io();
  }
}

Cache::replay_delta_ret
Cache::replay_delta(const delta_info_t &delta)
{
  // TODO
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}


}
