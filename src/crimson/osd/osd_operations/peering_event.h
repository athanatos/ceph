// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class OSD;
class ShardServices;
class PG;

class PeeringEvent : public OperationT<PeeringEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

  class PGPipeline {
    OrderedExclusivePhase await_map = {
      "PeeringEvent::PGPipeline::await_map"
    };
    OrderedExclusivePhase process = {
      "PeeringEvent::PGPipeline::process"
    };
    friend class PeeringEvent;
    friend class PGAdvanceMap;
  };

protected:
  PipelineHandle handle;
  PGPipeline &pp(PG &pg);

  ShardServices &shard_services;
  PeeringCtx ctx;
  pg_shard_t from;
  spg_t pgid;
  float delay = 0;
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
  virtual PeeringEvent::interruptible_future<> complete_rctx(Ref<PG>);
  virtual seastar::future<> complete_rctx_no_pg() { return seastar::now();}

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
  template <typename... Args>
  PeeringEvent(
    ShardServices &shard_services, const pg_shard_t &from, const spg_t &pgid,
    float delay, Args&&... args) :
    shard_services(shard_services),
    from(from),
    pgid(pgid),
    delay(delay),
    evt(std::forward<Args>(args)...)
  {}

  void _set_id(operation_id_t id) final { handle.set_id(id); }
  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);
};

class RemotePeeringEvent : public PeeringEvent {
protected:
  crimson::net::ConnectionRef conn;

  void on_pg_absent() final;
  PeeringEvent::interruptible_future<> complete_rctx(Ref<PG> pg) override;
  seastar::future<> complete_rctx_no_pg() override;

public:
  template <typename... Args>
  RemotePeeringEvent(crimson::net::ConnectionRef conn, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    conn(conn)
  {}

  static constexpr bool can_create() { return true; }
  auto get_create_info() { return std::move(evt.create_info); }
  spg_t get_pgid() const {
    return pgid;
  }
  ConnectionPipeline &get_connection_pipeline();
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return evt.get_epoch_sent(); }
};

class LocalPeeringEvent final : public PeeringEvent {
protected:
  Ref<PG> pg;

public:
  template <typename... Args>
  LocalPeeringEvent(Ref<PG> pg, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    pg(pg)
  {}

  seastar::future<> start();
  virtual ~LocalPeeringEvent();
};


}
