// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"

#include "crimson/osd/scrub/scrub_machine.h"

namespace crimson::osd::scrub {

Crash::Crash(my_context ctx) : ScrubState(ctx)
{
  ceph_abort("Crash state impossible");
}

void PrimaryActive::exit()
{
  if (local_reservation_held) {
    get_scrub_context().cancel_local_reservation();
  }
  if (remote_reservations_held) {
    // TODO: guard from interval change
    get_scrub_context().cancel_remote_reservations();
  }
}

GetLocalReservation::GetLocalReservation(my_context ctx) : ScrubState(ctx)
{
  context<PrimaryActive>().local_reservation_held = true;
  get_scrub_context().request_local_reservation();
}

GetRemoteReservations::GetRemoteReservations(my_context ctx) : ScrubState(ctx)
{
  context<PrimaryActive>().remote_reservations_held = true;
  get_scrub_context().request_remote_reservations();
}

Scrubbing::Scrubbing(my_context ctx) : ScrubState(ctx)
{
}

ChunkState::ChunkState(my_context ctx) : ScrubState(ctx)
{
}

void ChunkState::exit()
{
  if (range_reserved) {
    // TODO: guard from interval change
    get_scrub_context().release_range();
  }
}

GetRange::GetRange(my_context ctx) : ScrubState(ctx)
{
  get_scrub_context().request_range(context<Scrubbing>().current);
}

sc::result GetRange::react(const ScrubContext::request_range_complete_t &event)
{
  context<ChunkState>().range = event.value;
  return transit<ReserveRange>();
}

ReserveRange::ReserveRange(my_context ctx) : ScrubState(ctx)
{
  auto &cs = context<ChunkState>();
  cs.range_reserved = true;
  assert(cs.range);
  get_scrub_context().reserve_range(cs.range->start, cs.range->end);
}

ScanRange::ScanRange(my_context ctx) : ScrubState(ctx)
{
  const auto &cs = context<ChunkState>();
  assert(cs.range);
  get_scrub_context().scan_range(cs.range->start, cs.range->end);
}

sc::result ScanRange::react(const ScrubContext::scan_range_complete_t &event)
{
  auto &cs = context<ChunkState>();
  cs.scan_results = event.value;
  return discard_event();
}

};
