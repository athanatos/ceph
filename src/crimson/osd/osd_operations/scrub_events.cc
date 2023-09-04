// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg.h"
#include "crimson/osd/osd_connection_priv.h"
#include "scrub_events.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

template <class T>
PGPeeringPipeline &RemoteScrubEventBaseT<T>::pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

template <class T>
ConnectionPipeline &RemoteScrubEventBaseT<T>::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

template <class T>
seastar::future<> RemoteScrubEventBaseT<T>::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  return interruptor::with_interruption([this, pg, &shard_services] {
    logger().debug("{}: pg present", *that());
    return this->template enter_stage<interruptor>(pp(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
	PG_OSDMapGate::OSDMapBlocker::BlockingEvent
	>([this, pg](auto &&trigger) {
	  return pg->osdmap_gate.wait_for_map(
	    std::move(trigger), get_epoch());
	});
    }).then_interruptible([this, pg](auto) {
      return this->template enter_stage<interruptor>(pp(*pg).process);
    }).then_interruptible([this, pg] {
      return handle_event(*pg);
    });
  }, [this](std::exception_ptr ep) {
    logger().debug(
      "{}: interrupted with {}",
      *that(),
      ep);
  }, pg);
}

ScrubRequested::ifut<> ScrubRequested::handle_event(PG &pg)
{
  pg.scrubber.handle_scrub_requested();
  return seastar::now();
}

ScrubMessage::ifut<> ScrubMessage::handle_event(PG &pg)
{
  pg.scrubber.handle_scrub_message(*m);
  return seastar::now();
}

template class RemoteScrubEventBaseT<ScrubRequested>;
template class RemoteScrubEventBaseT<ScrubMessage>;

ScrubScan::ScrubScan(Ref<PG> pg, const hobject_t &begin, const hobject_t &end)
  : pg(pg), begin(begin), end(end) {}

void ScrubScan::print(std::ostream &) const
{
  // TODOSAM
}

void ScrubScan::dump_detail(ceph::Formatter *) const
{
  // TODOSAM
}

seastar::future<> ScrubScan::start()
{
  return interruptor::with_interruption([this] {
    return interruptor::make_interruptible(
      pg->shard_services.get_store().list_objects(
	pg->get_collection_ref(),
	ghobject_t(begin, ghobject_t::NO_GEN, pg->get_pgid().shard),
	ghobject_t(end, ghobject_t::NO_GEN, pg->get_pgid().shard),
	std::numeric_limits<uint64_t>::max())
    ).then_interruptible([this](auto &&result) {
      auto [objects, _] = std::move(result);
      return interruptor::do_for_each(
	objects,
	[this](auto &obj) {
	  return scan_object(obj);
	});
    });
  }, [this](std::exception_ptr ep) {
    logger().debug(
      "{}: interrupted with {}",
      *this,
      ep);
  }, pg);
}

ScrubScan::interruptible_future<> ScrubScan::scan_object(
  const ghobject_t &obj)
{
  return seastar::now();
}

ScrubScan::~ScrubScan() {}

}
