// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"

namespace ceph::osd {

struct OSDConnectionPriv : public ceph::net::Connection::priv {
  OrderedPipelinePhase connection_to_map = {
    "OSDConnectionPriv::connection_to_map"
  };
  OrderedPipelinePhase map_to_pg {
    "OSDConnectionPriv::map_to_pg"
  };
};

OSDConnectionPriv &get_osd_priv(ceph::net::Connection *conn) {
  if (!conn->get_priv()) {
    conn->set_priv(std::make_unique<OSDConnectionPriv>());
  }
  return *conn->get_priv();
}

}
