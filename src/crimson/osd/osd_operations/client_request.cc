// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/client_request.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

ClientRequest::ClientRequest(OSD &osd, Ref<MOSDOp> &&m)
  : osd(osd), m(m)
{}

void ClientRequest::print(std::ostream &lhs) const
{
}

void ClientRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("PeeringEvent");
  f->close_section();
}

seastar::future<> ClientRequest::start()
{
  logger().debug("{}: start", *this);

#if 0
  osd.wait_for_map(m->get_map_epoch()).then([this](epoch_t epoch) {
    return get_or_create_pg(m->get_spg(), epoch);
  }).then([this](Ref<PG> pg) {
    pg->handle_op(conn, std::move(m));
  });
    if (auto found = pgs.find(m->get_spg()); found != pgs.end()) {
      return found->second->
    } else if (osdmap->is_up_acting_osd_shard(m->get_spg(), whoami)) {
      logger().info("no pg, should exist e{}, will wait", epoch);
      // todo, wait for peering, etc
      return seastar::now();
    } else {
      logger().info("no pg, shouldn't exist e{}, dropping", epoch);
      // todo: share map with client
      return seastar::now();
    }
  });
#endif
  return seastar::make_ready_future();
}

}
