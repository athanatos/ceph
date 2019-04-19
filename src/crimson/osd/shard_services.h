// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/LogClient.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"

namespace ceph::net {
  class Messenger;
}

namespace ceph::mgr {
  class Client;
}

namespace ceph::osd {

/**
 * Represents services available to each PG
 */
class ShardServices {
  ceph::net::Messenger &cluster_msgr;
  ceph::net::Messenger &public_msgr;
  ceph::mon::Client &monc;
  ceph::mgr::Client &mgrc;
  
  CephContext cct;

  ShardServices(
    ceph::net::Messenger &cluster_msgr,
    ceph::net::Messenger &public_msgr,
    ceph::mon::Client &monc,
    ceph::mgr::Client &mgrc)
    : cluster_msgr(cluster_msgr),
      public_msgr(public_msgr),
      monc(monc),
      mgrc(mgrc) {}
};

}
