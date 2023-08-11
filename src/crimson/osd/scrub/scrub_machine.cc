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
    get_scrub_context().foreach_remote_id_to_scrub([this](const auto &id) {
      get_scrub_context().cancel_remote_reservation(id);
    });
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
  get_scrub_context().foreach_remote_id_to_scrub([this](const auto &id) {
    get_scrub_context().request_remote_reservation(id);
    ++waiting_on;
  });
}

sc::result GetRemoteReservations::react(
  const ScrubContext::request_remote_reservations_complete_t &)
{
  ceph_assert(waiting_on > 0);
  --waiting_on;
  if (waiting_on == 0) {
    return transit<Scrubbing>();
  } else {
    return discard_event();
  }
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
  ceph_assert(context<ChunkState>().range);
  const auto &range = context<ChunkState>().range.value();
  get_scrub_context().foreach_remote_id_to_scrub([this, &range](const auto &id) {
    get_scrub_context().scan_range(id, range.start, range.end);
    waiting_on++;
  });
}

sc::result ScanRange::react(const ScrubContext::scan_range_complete_t &event)
{
  auto [_, inserted] = maps.insert(event.value);
  ceph_assert(inserted);
  ceph_assert(waiting_on > 0);
  --waiting_on;

  if (waiting_on > 0) {
    return discard_event();
  } else {
    ceph_assert(context<ChunkState>().range);
    get_scrub_context().emit_chunk_result(
      *(context<ChunkState>().range),
      validate_chunk(
	get_scrub_context().get_policy(),
	maps));
    context<Scrubbing>().advance_current(
      context<ChunkState>().range->end);
    return transit<ChunkState>();
  }
}

void ReplicaActive::exit()
{
  if (reservation_held) {
    get_scrub_context().replica_cancel_local_reservation();
  }
}

ReplicaGetLocalReservation::ReplicaGetLocalReservation(my_context ctx)
  : ScrubState(ctx)
{
  context<ReplicaActive>().reservation_held = true;
  get_scrub_context().replica_request_local_reservation();
}


sc::result ReplicaGetLocalReservation::react(
  const ScrubContext::replica_request_local_reservation_complete_t &event)
{
  get_scrub_context().replica_confirm_reservation();
  return transit<ReplicaAwaitScan>();
}

};
