// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "logmissing_request.h"

#include "common/Formatter.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/replicated_backend.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

SET_SUBSYS(osd);

namespace crimson::osd {

PGPCTRequest::PGPCTRequest(crimson::net::ConnectionRef&& conn,
		       Ref<MOSDPGPCT> &&req)
  : l_conn{std::move(conn)},
    req{std::move(req)}
{}

void PGPCTRequest::print(std::ostream& os) const
{
  os << "PGPCTRequest("
     << " req=" << *req
     << ")";
}

void PGPCTRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("PGPCTRequest");
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("pg_committed_to") << req->pg_committed_to;
  f->close_section();
}

ConnectionPipeline &PGPCTRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).client_request_conn_pipeline;
}

PerShardPipeline &PGPCTRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

ClientRequest::PGPipeline &PGPCTRequest::client_pp(PG &pg)
{
  return pg.request_pg_pipeline;
}

seastar::future<> PGPCTRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(PGPCTRequest::with_pg);
  DEBUGI("{}: PGPCTRequest::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    LOG_PREFIX(PGPCTRequest::with_pg);
    DEBUGI("{}: pg present", *this);
    return this->template enter_stage<interruptor>(client_pp(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
        PG_OSDMapGate::OSDMapBlocker::BlockingEvent
      >([this, pg](auto &&trigger) {
        return pg->osdmap_gate.wait_for_map(
          std::move(trigger), req->min_epoch);
      });
    }).then_interruptible([this, pg](auto) {
      /* pgpct_request and replicated_request should really be treated uniformly.
       * do_pct below should be refactored into a general do_backend_request
       * which dispatches into the backend implementation -- that will also
       * simplify handling the several primary->replica ec messages.  For now,
       * we'll just do a static_cast and follow up later with a cleaner refactor.
       */
      static_cast<ReplicatedBackend&>(*(pg->backend)).do_pct(*req);
      logger().debug("{}: complete", *this);
      return handle.complete();
    });
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pg).finally([this, ref=std::move(ref)] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

}
