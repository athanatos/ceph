// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

ClientRequest::ClientRequest(
  OSD &osd, ceph::net::ConnectionRef conn, Ref<MOSDOp> &&m)
  : osd(osd), conn(conn), m(m)
{}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << *m;
}

void ClientRequest::dump_detail(Formatter *f) const
{
}

ClientRequest::ConnectionPipeline &ClientRequest::cp()
{
  return get_osd_priv(conn.get()).client_request_conn_pipeline;
}

ClientRequest::PGPipeline &ClientRequest::pp(PG &pg)
{
  return pg.client_request_pg_pipeline;
}

bool ClientRequest::is_pg_op() const
{
  return std::any_of(
    begin(m->ops), end(m->ops),
    [](auto& op) { return ceph_osd_op_type_pg(op.op.op); });
}

seastar::future<> ClientRequest::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  return with_blocking_future(handle.enter(cp().await_map))
    .then([this]() {
      return with_blocking_future(osd.osdmap_gate.wait_for_map(m->get_min_epoch()));
    }).then([this](epoch_t epoch) {
      return with_blocking_future(handle.enter(cp().get_pg));
    }).then([this] {
      return with_blocking_future(osd.wait_for_pg(m->get_spg()));
    }).then([this, ref=std::move(ref)](Ref<PG> pg) {
      return seastar::do_with(
	std::move(pg), std::move(ref), [this](auto pg, auto op) {
	  return with_blocking_future(
	    handle.enter(pp(*pg).await_map)
	  ).then([this, &pg]() mutable {
	    return with_blocking_future(
	      pg->osdmap_gate.wait_for_map(m->get_map_epoch()));
	  }).then([this, &pg](auto) mutable {
	    return with_blocking_future(
	      handle.enter(pp(*pg).wait_for_active));
	  }).then([this, &pg]() mutable {
	    return pg->wait_for_active();
	    //return with_blocking_future(pg->wait_for_active_bf());
	  }).then([this, &pg]() mutable {
	    if (m->finish_decode()) {
	      m->clear_payload();
	    }
	    if (is_pg_op()) {
	      return process_pg_op(pg);
	    } else {
	      return process_op(pg);
	    }
	  });
	});
    });
}

seastar::future<> ClientRequest::process_pg_op(
  Ref<PG> &pg)
{
  return pg->do_pg_ops(m)
    .then([this](Ref<MOSDOpReply> reply) {
      return conn->send(reply);
    });
}

seastar::future<> ClientRequest::process_op(
  Ref<PG> &pg)
{
  return with_blocking_future(
    handle.enter(pp(*pg).get_obc)
  ).then([this, pg]() {
    return pg->with_locked_obc(
      m, this,
      [this, pg](auto obc) {
	return with_blocking_future(handle.enter(pp(*pg).process)
	).then([this, pg, obc]() {
	  return pg->do_osd_ops(m, obc);
	}).then([this](Ref<MOSDOpReply> reply) {
	  return conn->send(reply);
	});
      });
  });
}

}
