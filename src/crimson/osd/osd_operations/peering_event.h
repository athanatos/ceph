// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"
#include "crimson/osd/pg.h"

namespace ceph::osd {

class OSD;

class PeeringEvent : public OperationT<PeeringEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

protected:
  ShardServices &shard_services;
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
  virtual seastar::future<Ref<PG>> get_pg() = 0;

public:
  template <typename... Args>
  PeeringEvent(
    ShardServices &shard_services, const pg_shard_t &from, const spg_t &pgid,
    Args&&... args) :
    shard_services(shard_services),
    from(from),
    pgid(pgid),
    evt(std::forward<Args>(args)...)
  {}


  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
  seastar::future<> start();
};

class RemotePeeringEvent : public PeeringEvent {
protected:
  OSD &osd;

  seastar::future<Ref<PG>> get_pg() final;

public:
  template <typename... Args>
  RemotePeeringEvent(OSD &osd, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    osd(osd)
  {}
};

class LocalPeeringEvent final : public PeeringEvent {
protected:
  seastar::future<Ref<PG>> get_pg() final {
    return seastar::make_ready_future<Ref<PG>>(pg);
  }

  Ref<PG> pg;

public:
  template <typename... Args>
  LocalPeeringEvent(Ref<PG> pg, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    pg(pg)
  {}
};


}
