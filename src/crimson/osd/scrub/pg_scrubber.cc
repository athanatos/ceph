// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/pg.h"
#include "pg_scrubber.h"

namespace crimson::osd::scrub {

void PGScrubber::on_primary_activate()
{
}

void PGScrubber::on_interval_change()
{
}

void PGScrubber::handle_scrub_requested()
{
  if (pg.peering_state.is_active()) {
    machine.process_event(StartScrub{});
  }
}

void PGScrubber::handle_scrub_message(Message &m)
{
  switch (m.get_type()) {
  case MSG_OSD_REP_SCRUB:
    // do scan
  default:
    ceph_assert(is_scrub_message(m));
  }
}

const std::set<pg_shard_t> &PGScrubber::get_ids_to_scrub() const
{
  return pg.peering_state.get_actingset();
}

void PGScrubber::request_range(const hobject_t &start)
{
}

eversion_t PGScrubber::reserve_range(const hobject_t &start, const hobject_t &end)
{
  return eversion_t{};
}

void PGScrubber::release_range()
{
}

void PGScrubber::scan_range(
  pg_shard_t target,
  const hobject_t &start,
  const hobject_t &end)
{
}

void PGScrubber::await_update(const eversion_t &version)
{
}

void PGScrubber::generate_and_submit_chunk_result(
  ScrubMap &map)
{
}

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
}

}
