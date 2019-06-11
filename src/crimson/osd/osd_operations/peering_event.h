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
class ShardServices;
class PG;

class PeeringEvent : public OperationT<PeeringEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

  class PGPipeline {
    OrderedPipelinePhase await_map = {
      "PeeringEvent::PGPipeline::await_map"
    };
    OrderedPipelinePhase process = {
      "PeeringEvent::PGPipeline::process"
    };
    friend class PeeringEvent;
  };

protected:
  OrderedPipelinePhase::Handle handle;
  PGPipeline &pp(PG &pg);

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
  ceph::net::ConnectionRef conn;

  seastar::future<Ref<PG>> get_pg() final;

public:
  class ConnectionPipeline {
    OrderedPipelinePhase await_map = {
      "PeeringRequest::ConnectionPipeline::await_map"
    };
    OrderedPipelinePhase get_pg = {
      "PeeringRequest::ConnectionPipeline::get_pg"
    };
    friend class RemotePeeringEvent;
  };

  template <typename... Args>
  RemotePeeringEvent(OSD &osd, ceph::net::ConnectionRef conn, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    osd(osd),
    conn(conn)
  {}

private:
  ConnectionPipeline &cp();
};

class LocalPeeringEvent final : public PeeringEvent {
protected:
  seastar::future<Ref<PG>> get_pg() final;

  Ref<PG> pg;

public:
  template <typename... Args>
  LocalPeeringEvent(Ref<PG> pg, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    pg(pg)
  {}

  virtual ~LocalPeeringEvent();
};


}
