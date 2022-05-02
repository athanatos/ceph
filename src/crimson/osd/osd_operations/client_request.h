// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;
class ShardServices;

class ClientRequest final : public OperationT<ClientRequest>,
                            private CommonClientRequest {
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDOp> m;
  OpInfo op_info;
  PipelineHandle handle;

public:
  class PGPipeline : public CommonPGPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::PGPipeline::await_map"
    };
    OrderedConcurrentPhase wait_repop = {
      "ClientRequest::PGPipeline::wait_repop"
    };
    OrderedExclusivePhase send_reply = {
      "ClientRequest::PGPipeline::send_reply"
    };
    friend class ClientRequest;
  };

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, crimson::net::ConnectionRef, Ref<MOSDOp> &&m);
  ~ClientRequest();

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return m->get_spg();
  }
  ConnectionPipeline &get_connection_pipeline();
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return m->get_min_epoch(); }

  seastar::future<seastar::stop_iteration> with_pg_int(
    ShardServices &shard_services, Ref<PG> pg);
public:
  bool same_session_and_pg(const ClientRequest& other_op) const;

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pgref);
private:
  template <typename FuncT>
  interruptible_future<> with_sequencer(FuncT&& func);
  auto reply_op_error(Ref<PG>& pg, int err);

  enum class seq_mode_t {
    IN_ORDER,
    OUT_OF_ORDER
  };

  interruptible_future<seq_mode_t> do_process(
    Ref<PG>& pg,
    crimson::osd::ObjectContextRef obc);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_pg_op(
    Ref<PG> &pg);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition, seq_mode_t> process_op(
    Ref<PG> &pg);
  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  class OpSequencer& sequencer;
  // a tombstone used currently by OpSequencer. In the future it's supposed
  // to be replaced with a reusage of OpTracking facilities.
  bool finished = false;
  friend class OpSequencer;

  template <typename Errorator>
  using interruptible_errorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      Errorator>;
private:
  bool is_misdirected(const PG& pg) const;
};

}
