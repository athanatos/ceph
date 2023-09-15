// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/scrub_events.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "pg_scrubber.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

void PGScrubber::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();
}

void PGScrubber::on_primary_active_clean()
{
  LOG_PREFIX(PGScrubber::on_primary_active_clean);
  DEBUGDPP("", pg);
  machine.process_event(PrimaryActivate{});
}

void PGScrubber::on_replica_activate()
{
  LOG_PREFIX(PGScrubber::on_replica_activate);
  DEBUGDPP("", pg);
  machine.process_event(ReplicaActivate{});
}

void PGScrubber::on_interval_change()
{
  LOG_PREFIX(PGScrubber::on_interval_change);
  DEBUGDPP("", pg);
  /* Once reservations and scheduling are introduced, we'll need an
   * IntervalChange event to drop remote resources (they'll be automatically
   * released on the other side */
  machine.process_event(Reset{});
  waiting_for_update = std::nullopt;
  ceph_assert(!blocked);
}

void PGScrubber::on_log_update(eversion_t v)
{
  LOG_PREFIX(PGScrubber::on_interval_change);
  if (waiting_for_update && v >= *waiting_for_update) {
    DEBUGDPP("waiting_for_update: {}, v: {}", pg, *waiting_for_update, v);
    machine.process_event(await_update_complete_t{});
    waiting_for_update = std::nullopt;
  }
}

void PGScrubber::handle_scrub_requested(bool deep)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  DEBUGDPP("deep: {}", pg, deep);
  machine.process_event(StartScrub{deep});
}

void PGScrubber::handle_scrub_message(Message &_m)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  switch (_m.get_type()) {
  case MSG_OSD_REP_SCRUB: {
    MOSDRepScrub &m = *static_cast<MOSDRepScrub*>(&_m);
    DEBUGDPP("MOSDRepScrub: {}", pg, m);
    machine.process_event(ReplicaScan{
	m.start, m.end, m.scrub_from, m.deep
      });
    break;
  }
  case MSG_OSD_REP_SCRUBMAP: {
    MOSDRepScrubMap &m = *static_cast<MOSDRepScrubMap*>(&_m);
    DEBUGDPP("MOSDRepScrubMap: {}", pg, m);
    ScrubMap map;
    auto iter = m.scrub_map_bl.cbegin();
    ::decode(map, iter);
    machine.process_event(scan_range_complete_t{
	std::make_pair(m.from, std::move(map))
      });
    break;
  }
  default:
    DEBUGDPP("invalid message: {}", pg, _m);
    ceph_assert(is_scrub_message(_m));
  }
}

PGScrubber::ifut<> PGScrubber::wait_scrub(
  PGScrubber::BlockingEvent::TriggerI&& trigger,
  const hobject_t &hoid)
{
  LOG_PREFIX(PGScrubber::wait_scrub);
  if (blocked && (hoid >= blocked->begin) && (hoid < blocked->end)) {
    DEBUGDPP("blocked: {}, hoid: {}", pg, *blocked, hoid);
    return trigger.maybe_record_blocking(
      blocked->p.get_shared_future(),
      *this);
  } else {
    return seastar::now();
  }
}

void PGScrubber::notify_scrub_start(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_start);
  DEBUGDPP("deep: {}", pg, deep);
  pg.peering_state.state_set(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_set(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}

void PGScrubber::notify_scrub_end(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_end);
  DEBUGDPP("deep: {}", pg, deep);
  pg.peering_state.state_clear(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_clear(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}

const std::set<pg_shard_t> &PGScrubber::get_ids_to_scrub() const
{
  return pg.peering_state.get_actingset();
}

void PGScrubber::request_range(const hobject_t &start)
{
  LOG_PREFIX(PGScrubber::request_range);
  DEBUGDPP("start: {}", pg, start);
  using crimson::common::local_conf;
  std::ignore = ifut<>(seastar::yield()
  ).then_interruptible([this, start] {
    return pg.shard_services.get_store().list_objects(
      pg.get_collection_ref(),
      ghobject_t(start, ghobject_t::NO_GEN, pg.get_pgid().shard),
      ghobject_t::get_max(),
      local_conf().get_val<uint64_t>("osd_scrub_chunk_max"));
  }).then_interruptible([FNAME, this, start](auto ret) {
    auto &[_, next] = ret;
    DEBUGDPP("returning start, end: {}, {}", pg, start, next.hobj);
    machine.process_event(request_range_complete_t{start, next.hobj});
  });
}

/* TODOSAM: This isn't actually enough.  Here, classic would
 * hold the pg lock from the wait_scrub through to IO submission.
 * ClientRequest, however, isn't in the processing ExclusivePhase
 * bit yet, and so this check may miss ops between the wait_scrub
 * check and adding the IO to the log. */

void PGScrubber::reserve_range(const hobject_t &start, const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::reserve_range);
  DEBUGDPP("start: {}, end: {}", pg, start, end);
  std::ignore = ifut<>(seastar::yield()
  ).then_interruptible([this] {
    return pg.background_io_mutex.lock();
  }).then_interruptible([this, start, end] {
    ceph_assert(!blocked);
    blocked = blocked_range_t{start, end};
    auto& log = pg.peering_state.get_pg_log().get_log().log;
    auto p = find_if(
      log.crbegin(), log.crend(),
      [this, &start, &end](const auto& e) -> bool {
	return e.soid >= start && e.soid < end;
      });
    
    if (p == log.crend()) {
      return machine.process_event(reserve_range_complete_t{eversion_t{}});
    } else {
      return machine.process_event(reserve_range_complete_t{p->version});
    }
  });
}

void PGScrubber::release_range()
{
  LOG_PREFIX(PGScrubber::release_range);
  ceph_assert(blocked);
  DEBUGDPP("blocked: {}", pg, *blocked);
  pg.background_io_mutex.unlock();
  blocked->p.set_value();
  blocked = std::nullopt;
}

void PGScrubber::scan_range(
  pg_shard_t target,
  eversion_t version,
  bool deep,
  const hobject_t &start,
  const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::scan_range);
  DEBUGDPP("target: {}, version: {}, deep: {}, start: {}, end: {}",
	   pg, version, deep, start, end);
  if (target == pg.get_pg_whoami()) {
    std::ignore = pg.shard_services.start_operation<ScrubScan>(
      &pg, deep, true /* local */, start, end
    );
  } else {
    std::ignore = pg.shard_services.send_to_osd(
      pg.get_primary().osd,
      crimson::make_message<MOSDRepScrub>(
	spg_t(pg.get_pgid().pgid, target.shard),
	version,
	pg.get_osdmap_epoch(),
	pg.get_osdmap_epoch(),
	start,
	end,
	deep,
	false /* allow preemption -- irrelevant for replicas TODO */,
	64 /* priority, TODO */,
	false /* high_priority TODO */),
      pg.get_osdmap_epoch());
  }
}

// TODOSAM: probably can't send an event syncronously
void PGScrubber::await_update(const eversion_t &version)
{
  LOG_PREFIX(PGScrubber::await_update);
  DEBUGDPP("version: {}", pg, version);
  ceph_assert(!waiting_for_update);
  waiting_for_update = version;
  auto& log = pg.peering_state.get_pg_log().get_log().log;
  on_log_update(log.empty() ? eversion_t() : log.rbegin()->version);
}

void PGScrubber::generate_and_submit_chunk_result(
  const hobject_t &begin,
  const hobject_t &end,
  bool deep)
{
  LOG_PREFIX(PGScrubber::generate_and_submit_chunk_result);
  DEBUGDPP("begin: {}, end: {}, deep: {}", pg, begin, end, deep);
  std::ignore = pg.shard_services.start_operation<ScrubScan>(
    &pg, deep, false /* not local */, begin, end
  );
}

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
  LOG_PREFIX(PGScrubber::emit_chunk_result);
  DEBUGDPP("", pg);
  // TODO: repair and updating durable scrub results
}

void PGScrubber::emit_scrub_result(
  bool deep,
  object_stat_sum_t in_stats)
{
  LOG_PREFIX(PGScrubber::emit_scrub_result);
  DEBUGDPP("", pg);
  pg.peering_state.update_stats(
    [this, deep, &in_stats](auto &history, auto &pg_stats) {
      iterate_scrub_maintained_stats(
	[&pg_stats, &in_stats](const auto &name, auto statptr) {
	  pg_stats.stats.sum.*statptr = in_stats.*statptr;
	});
      iterate_scrub_checked_stats(
	[&pg_stats, &in_stats](
	  const auto &name, auto statptr, const auto &invalid_predicate) {
	  if (!invalid_predicate(pg_stats) &&
	      (in_stats.*statptr != pg_stats.stats.sum.*statptr)) {
	    ++pg_stats.stats.sum.num_shallow_scrub_errors;
	  }
	});
      history.last_scrub = pg.peering_state.get_info().last_update;
      auto now = ceph_clock_now();
      history.last_scrub_stamp = now;
      if (deep) {
	history.last_deep_scrub_stamp = now;
      }
      return false; // notify_scrub_end will flush stats to osd
    });
}

}
