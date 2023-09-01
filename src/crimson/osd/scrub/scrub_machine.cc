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
  cs.version = get_scrub_context().reserve_range(cs.range->start, cs.range->end);
  get_scrub_context().await_update(cs.version);
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

};
