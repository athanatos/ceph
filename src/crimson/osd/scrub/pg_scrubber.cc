// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/pg.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "pg_scrubber.h"

namespace crimson::osd::scrub {

void PGScrubber::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();
}

void PGScrubber::on_primary_active_clean()
{
  machine.process_event(PrimaryActivate{});
}

void PGScrubber::on_replica_activate()
{
  machine.process_event(ReplicaActivate{});
}

void PGScrubber::on_interval_change()
{
  /* Once reservations and scheduling are introduced, we'll need an
   * IntervalChange event to drop remote resources (they'll be automatically
   * released on the other side */
  machine.process_event(Reset{});
  ceph_assert(!blocked);
}

void PGScrubber::handle_scrub_requested()
{
  machine.process_event(StartScrub{});
}

void PGScrubber::handle_scrub_message(Message &_m)
{
  switch (_m.get_type()) {
  case MSG_OSD_REP_SCRUB: {
    MOSDRepScrub &m = *static_cast<MOSDRepScrub*>(&_m);
    machine.process_event(ReplicaScan{
	m.start, m.end, m.scrub_from, m.deep
      });
    break;
  }
  case MSG_OSD_REP_SCRUBMAP: {
    MOSDRepScrubMap &m = *static_cast<MOSDRepScrubMap*>(&_m);
    ScrubMap map;
    auto iter = m.scrub_map_bl.cbegin();
    ::decode(map, iter);
    machine.process_event(scan_range_complete_t{
	std::make_pair(m.from, std::move(map))
      });
    break;
  }
  default:
    ceph_assert(is_scrub_message(_m));
  }
}

seastar::future<>
PGScrubber::wait(
  const hobject_t &hoid,
  PGScrubber::BlockingEvent::TriggerI&& trigger)
{
  return seastar::now();
#if 0
  if (pg->get_peering_state().is_active()) {
    return seastar::now();
  } else {
    return trigger.maybe_record_blocking(p.get_shared_future(), *this);
  }
#endif
}

PGScrubber::ifut<> PGScrubber::wait_scrub(const hobject_t &hoid)
{
  return seastar::now();
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
  ceph_assert(blocked);
  blocked->p.set_value();
  blocked = std::nullopt;
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
  const hobject_t &begin,
  const hobject_t &end,
  bool deep)
{
}

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
}

}
