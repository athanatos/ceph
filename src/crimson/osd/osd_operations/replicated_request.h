// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDRepOp.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSD;
class PG;

class RepRequest final : public OperationT<RepRequest> {
public:
  class PGPipeline {
    OrderedExclusivePhase await_map = {
      "RepRequest::PGPipeline::await_map"
    };
    OrderedExclusivePhase process = {
      "RepRequest::PGPipeline::process"
    };
    friend RepRequest;
  };
  static constexpr OperationTypeCode type = OperationTypeCode::replicated_request;
  RepRequest(crimson::net::ConnectionRef&&, Ref<MOSDRepOp>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return req->get_spg();
  }
  ConnectionPipeline &get_connection_pipeline();
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return req->get_min_epoch(); }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

private:
  PGPipeline &pp(PG &pg);

  crimson::net::ConnectionRef conn;
  Ref<MOSDRepOp> req;
  PipelineHandle handle;
};

}
