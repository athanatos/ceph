// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"

namespace ceph::osd {

struct OSDConnectionPriv : public ceph::net::Connection::priv {
  ClientRequest::ConnectionPipeline client_request_conn_pipeline;
  RemotePeeringEvent::ConnectionPipeline peering_request_conn_pipeline;
};

static OSDConnectionPriv &get_osd_priv(ceph::net::Connection *conn) {
  if (!conn->has_priv()) {
    conn->set_priv(std::make_unique<OSDConnectionPriv>());
  }
  return static_cast<OSDConnectionPriv&>(conn->get_priv());
}

}
