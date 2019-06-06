// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/peering_event.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

void PeeringEvent::print(std::ostream &lhs) const
{
  lhs << "PeeringEvent("
      << "from=" << from
      << " pgid=" << pgid
      << " sent=" << evt.get_epoch_sent()
      << " requested=" << evt.get_epoch_requested()
      << " evt=" << evt.get_desc()
      << ")";
}

void PeeringEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("PeeringEvent");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  f->dump_int("sent", evt.get_epoch_sent());
  f->dump_int("requested", evt.get_epoch_requested());
  f->dump_string("evt", evt.get_desc());
  f->close_section();
}

seastar::future<> PeeringEvent::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  osd.osdmap_gate.wait_for_map(this, evt.get_epoch_sent())
    .then([this](auto epoch) {
      logger().debug("{}: got map {}", *this, epoch);
      return osd.get_or_create_pg(
	pgid, evt.get_epoch_sent(), std::move(evt.create_info));
    }).then([this](Ref<PG> pg) {
      if (!pg) {
	logger().debug("{}: pg absent, did not create", *this);
	on_pg_absent();
      } else {
	logger().debug("{}: pg present", *this);
	pg->do_peering_event(evt, ctx);
      }
      return complete_rctx(pg);
    }).then([this, ref=std::move(ref)] {
      logger().debug("{}: complete", *this);
    });

  return seastar::make_ready_future();
}

void PeeringEvent::on_pg_absent()
{
  logger().debug("{}: pg absent, dropping", *this);
}

seastar::future<> PeeringEvent::complete_rctx(Ref<PG> pg)
{
  logger().debug("{}: submitting ctx", *this);
  return osd.get_shard_services().dispatch_context(
    pg->get_collection_ref(),
    std::move(ctx));
}

}
