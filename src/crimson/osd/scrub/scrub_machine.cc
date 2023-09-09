// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"

#include "crimson/osd/scrub/scrub_machine.h"

namespace crimson::osd::scrub {

Crash::Crash(my_context ctx) : ScrubState(ctx)
{
  ceph_abort("Crash state impossible");
}

Inactive::Inactive(my_context ctx) : ScrubState(ctx)
{
}

PrimaryActive::PrimaryActive(my_context ctx) : ScrubState(ctx)
{
}

AwaitScrub::AwaitScrub(my_context ctx) : ScrubState(ctx)
{
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
  return transit<WaitUpdate>();
}

WaitUpdate::WaitUpdate(my_context ctx) : ScrubState(ctx)
{
  auto &cs = context<ChunkState>();
  cs.range_reserved = true;
  assert(cs.range);
  get_scrub_context().reserve_range(cs.range->start, cs.range->end);
}

sc::result WaitUpdate::react(const ScrubContext::reserve_range_complete_t &event)
{
  auto &cs = context<ChunkState>();
  cs.version = event.value;
  return transit<ScanRange>();
}

ScanRange::ScanRange(my_context ctx) : ScrubState(ctx)
{
  ceph_assert(context<ChunkState>().range);
  const auto &cs = context<ChunkState>();
  const auto &range = cs.range.value();
  get_scrub_context(
  ).foreach_remote_id_to_scrub([this, &range, &cs](const auto &id) {
    get_scrub_context().scan_range(id, cs.version, range.start, range.end);
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

sc::result ReplicaChunkState::react(const ReplicaScan &event)
{
  start = event.value.start;
  end = event.value.end;
  version = event.value.version;
  deep = event.value.deep;
  return forward_event();
}

sc::result ReplicaWaitUpdate::react(const ReplicaScan &event)
{
  get_scrub_context().await_update(event.value.version);
  return forward_event();
}

ReplicaScanChunk::ReplicaScanChunk(my_context ctx) : ScrubState(ctx)
{
  auto &cs = context<ReplicaChunkState>();
  get_scrub_context().generate_and_submit_chunk_result(
    cs.start,
    cs.end,
    cs.deep);
}

};
