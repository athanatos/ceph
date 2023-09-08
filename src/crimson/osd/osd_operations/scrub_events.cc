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

ScrubScan::ScrubScan(
  Ref<PG> pg, bool deep, const hobject_t &begin, const hobject_t &end)
  : pg(pg), deep(deep), begin(begin), end(end) {}

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
  // legacy value, unused
  ret.valid_through = pg->get_info().last_update;

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
  auto &entry = ret.objects[obj.hobj];
  return interruptor::make_interruptible(
    pg->shard_services.get_store().stat(
      pg->get_collection_ref(),
      obj)
  ).then_interruptible([this, &obj, &entry](struct stat obj_stat) {
    entry.size = obj_stat.st_size;
    return pg->shard_services.get_store().get_attrs(
      pg->get_collection_ref(),
      obj);
  }).safe_then_interruptible([this, &entry](auto &&attrs) {
    for (auto &i : attrs) {
      i.second.rebuild();
      entry.attrs.emplace(i.first, *(i.second.begin()));
    }
  }).handle_error_interruptible(
    ct_error::all_same_way([this, &entry](auto e) {
      entry.stat_error = true;
    })
  ).then_interruptible([this, &obj] {
    if (deep) {
      return deep_scan_object(obj);
    } else {
      return interruptor::now();
    }
  });
    
}

ScrubScan::interruptible_future<> ScrubScan::deep_scan_object(
  const ghobject_t &obj)
{
  using crimson::common::local_conf;
  struct obj_scrub_progress_t {
    // nullopt once complete
    std::optional<uint64_t> offset;
    ceph::buffer::hash data_hash{std::numeric_limits<uint32_t>::max()};

    bool header_done = false;
    std::optional<std::string> next_key;
    bool keys_done = false;
    ceph::buffer::hash omap_hash{std::numeric_limits<uint32_t>::max()};
  };
  auto &entry = ret.objects[obj.hobj];
  return interruptor::repeat(
    [this, progress = std::make_unique<obj_scrub_progress_t>(), &obj, &entry]()
    -> interruptible_future<seastar::stop_iteration> {
      if (progress->offset) {
	const auto stride = local_conf().get_val<uint64_t>(
	  "osd_deep_scrub_stride");
	return pg->shard_services.get_store().read(
	  pg->get_collection_ref(),
	  obj,
	  *(progress->offset),
	  stride
	).safe_then([this, stride, &progress, &entry](auto bl) {
	  progress->data_hash << bl;
	  if (bl.length() < stride) {
	    progress->offset = std::nullopt;
	    entry.digest = progress->data_hash.digest();
	  } else {
	    ceph_assert(stride == bl.length());
	    *(progress->offset) += stride;
	  }
	}).handle_error(
	  ct_error::all_same_way([this, &progress, &entry](auto e) {
	    entry.read_error = true;
	    progress->offset = std::nullopt;
	  })
	).then([] {
	  return interruptor::make_interruptible(
	    seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no));
	});
      } else if (!progress->header_done) {
	return pg->shard_services.get_store().omap_get_header(
	  pg->get_collection_ref(),
	  obj
	).safe_then([this, &progress, &entry](auto bl) {
	  progress->omap_hash << bl;
	}).handle_error(
	  ct_error::enodata::handle([] {}),
	  ct_error::all_same_way([this, &progress, &entry](auto e) {
	    entry.read_error = true;
	  })
	).then([&progress] {
	  progress->header_done = true;
	  return interruptor::make_interruptible(
	    seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no));
	});
      } else {
	return interruptor::make_interruptible(
	  seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes));
      }
    });
}

ScrubScan::~ScrubScan() {}

}
