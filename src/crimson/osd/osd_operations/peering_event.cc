// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include "messages/MOSDPGLog.h"

#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

template <class T>
void PeeringEvent<T>::print(std::ostream &lhs) const
{
  lhs << "PeeringEvent("
      << "from=" << from
      << " pgid=" << pgid
      << " sent=" << evt.get_epoch_sent()
      << " requested=" << evt.get_epoch_requested()
      << " evt=" << evt.get_desc()
      << ")";
}

template <class T>
void PeeringEvent<T>::dump_detail(Formatter *f) const
{
  f->open_object_section("PeeringEvent");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  f->dump_int("sent", evt.get_epoch_sent());
  f->dump_int("requested", evt.get_epoch_requested());
  f->dump_string("evt", evt.get_desc());
  f->close_section();
}


template <class T>
PGPeeringPipeline &PeeringEvent<T>::pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

template <class T>
seastar::future<> PeeringEvent<T>::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  if (!pg) {
    logger().warn("{}: pg absent, did not create", *this);
    on_pg_absent(shard_services);
    that()->get_handle().exit();
    return complete_rctx_no_pg(shard_services);
  }

  using interruptor = typename T::interruptor;
  return interruptor::with_interruption([this, pg, &shard_services] {
    logger().debug("{}: pg present", *this);
    return this->template enter_stage<interruptor>(pp(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
	PG_OSDMapGate::OSDMapBlocker::BlockingEvent
	>([this, pg](auto &&trigger) {
	  return pg->osdmap_gate.wait_for_map(
	    std::move(trigger), evt.get_epoch_sent());
	});
    }).then_interruptible([this, pg](auto) {
      return this->template enter_stage<interruptor>(pp(*pg).process);
    }).then_interruptible([this, pg] {
      // TODO: likely we should synchronize also with the pg log-based
      // recovery.
      return this->template enter_stage<interruptor>(
	BackfillRecovery::bp(*pg).process);
    }).then_interruptible([this, pg, &shard_services] {
      pg->do_peering_event(evt, ctx);
      that()->get_handle().exit();
      return complete_rctx(shard_services, pg);
    }).then_interruptible([this, pg, &shard_services]()
			  -> typename T::template interruptible_future<> {
      if (!pg->get_need_up_thru()) {
	return seastar::now();
      }
      logger().debug("{}: about to send_alive", *this);
      return shard_services.send_alive(pg->get_same_interval_since());
    }).then_interruptible([this, &shard_services] {
      logger().debug("{}: about to send_pg_temp", *this);
      return shard_services.send_pg_temp();
    }).then_interruptible([this] {
      logger().debug("{}: complete", *this);
    });
  }, [this](std::exception_ptr ep) {
    logger().debug("{}: interrupted with {}", *this, ep);
  }, pg);
}

template <class T>
void PeeringEvent<T>::on_pg_absent(ShardServices &)
{
  logger().debug("{}: pg absent, dropping", *this);
}

template <class T>
typename PeeringEvent<T>::template interruptible_future<>
PeeringEvent<T>::complete_rctx(ShardServices &shard_services, Ref<PG> pg)
{
  logger().debug("{}: submitting ctx", *this);
  return shard_services.dispatch_context(
    pg->get_collection_ref(),
    std::move(ctx));
}

ConnectionPipeline &RemotePeeringEvent::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

void RemotePeeringEvent::on_pg_absent(ShardServices &shard_services)
{
  if (auto& e = get_event().get_event();
      e.dynamic_type() == MQuery::static_type()) {
    const auto map_epoch =
      shard_services.get_map()->get_epoch();
    const auto& q = static_cast<const MQuery&>(e);
    const pg_info_t empty{spg_t{pgid.pgid, q.query.to}};
    if (q.query.type == q.query.LOG ||
	q.query.type == q.query.FULLLOG)  {
      auto m = crimson::make_message<MOSDPGLog>(q.query.from, q.query.to,
					     map_epoch, empty,
					     q.query.epoch_sent);
      ctx.send_osd_message(q.from.osd, std::move(m));
    } else {
      ctx.send_notify(q.from.osd, {q.query.from, q.query.to,
				   q.query.epoch_sent,
				   map_epoch, empty,
				   PastIntervals{}});
    }
  }
}

RemotePeeringEvent::interruptible_future<> RemotePeeringEvent::complete_rctx(
  ShardServices &shard_services,
  Ref<PG> pg)
{
  if (pg) {
    return PeeringEvent::complete_rctx(shard_services, pg);
  } else {
    return shard_services.dispatch_context_messages(std::move(ctx));
  }
}

seastar::future<> RemotePeeringEvent::complete_rctx_no_pg(
  ShardServices &shard_services)
{
  return shard_services.dispatch_context_messages(std::move(ctx));
}

seastar::future<> LocalPeeringEvent::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref)] {
    logger().debug("{}: complete", *ref);
  });
}


LocalPeeringEvent::~LocalPeeringEvent() {}

template class PeeringEvent<RemotePeeringEvent>;
template class PeeringEvent<LocalPeeringEvent>;

}
