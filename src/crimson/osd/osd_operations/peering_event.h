// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

namespace ceph::osd {

class OSD;

class PeeringEvent : public OperationT<PeeringEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

protected:
  OSD &osd;
  PeeringCtx ctx;
  pg_shard_t from;
  spg_t pgid;
  PGPeeringEvent evt;

  const pg_shard_t get_from() const {
    return from;
  }

  const spg_t get_pgid() const {
    return pgid;
  }

  const PGPeeringEvent &get_event() const {
    return evt;
  }

  virtual void on_pg_absent();
  virtual seastar::future<> complete_rctx(Ref<PG>);

public:
  template <typename... Args>
  PeeringEvent(OSD &osd, const pg_shard_t &from, const spg_t &pgid, Args&&... args) :
    osd(osd),
    from(from),
    pgid(pgid),
    evt(std::forward<Args>(args)...)
  {}


  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
  seastar::future<> start();
};

}
