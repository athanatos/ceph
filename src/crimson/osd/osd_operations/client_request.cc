// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

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
  return seastar::make_ready_future();
}

}
