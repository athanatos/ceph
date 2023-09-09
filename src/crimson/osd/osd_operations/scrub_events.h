// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/Formatter.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/scrub/pg_scrubber.h"
#include "osd/osd_types.h"
#include "peering_event.h"

namespace crimson::osd {

class PG;

template <typename T>
class RemoteScrubEventBaseT : public PhasedOperationT<T> {
  virtual void scrub_event_print(std::ostream &) const = 0;
  virtual void scrub_event_dump_detail(ceph::Formatter* f) const = 0;

  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  PipelineHandle handle;

  crimson::net::ConnectionRef conn;
  epoch_t epoch;
  spg_t pgid;

protected:
  using interruptor = InterruptibleOperation::interruptor;

  template <typename U=void>
  using ifut = InterruptibleOperation::interruptible_future<U>;

  virtual ifut<> handle_event(PG &pg) = 0;
public:
  RemoteScrubEventBaseT(
    crimson::net::ConnectionRef conn, epoch_t epoch, spg_t pgid)
    : conn(conn), epoch(epoch), pgid(pgid) {}

  void print(std::ostream &) const final {
    // TODO
  }
  void dump_detail(ceph::Formatter *) const final {
    // TODO
  }

  PGPeeringPipeline &pp(PG &pg);
  ConnectionPipeline &get_connection_pipeline();

  static constexpr bool can_create() { return false; }

  spg_t get_pgid() const {
    return pgid;
  }

  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return epoch; }

  seastar::future<crimson::net::ConnectionFRef> prepare_remote_submission() {
    assert(conn);
    return conn.get_foreign(
    ).then([this](auto f_conn) {
      conn.reset();
      return f_conn;
    });
  }
  void finish_remote_submission(crimson::net::ConnectionFRef _conn) {
    assert(!conn);
    conn = make_local_shared_foreign(std::move(_conn));
  }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    class TrackableOperationT<T>::StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    PGPeeringPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPeeringPipeline::Process::BlockingEvent,
    class TrackableOperationT<T>::CompletionEvent
  > tracking_events;
};

class ScrubRequested final : public RemoteScrubEventBaseT<ScrubRequested> {
  void scrub_event_print(std::ostream &) const final { /* TODO */ }
  void scrub_event_dump_detail(ceph::Formatter* f) const final { /* TODO */ }

protected:
  ifut<> handle_event(PG &pg) final;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_requested;

  template <typename... Args>
  ScrubRequested(Args&&... base_args)
    : RemoteScrubEventBaseT<ScrubRequested>(std::forward<Args>(base_args)...) {}
};

class ScrubMessage final : public RemoteScrubEventBaseT<ScrubMessage> {
  void scrub_event_print(std::ostream &) const final { /* TODO */ }
  void scrub_event_dump_detail(ceph::Formatter* f) const final { /* TODO */ }

  MessageRef m;
protected:
  ifut<> handle_event(PG &pg) final;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_message;

  template <typename... Args>
  ScrubMessage(MessageRef m, Args&&... base_args)
    : RemoteScrubEventBaseT<ScrubMessage>(std::forward<Args>(base_args)...),
      m(m) {
    ceph_assert(scrub::PGScrubber::is_scrub_message(*m));
  }
};

class ScrubScan : public TrackableOperationT<ScrubScan> {
  Ref<PG> pg;
  /// deep or shallow scrub
  const bool deep;

  /// true: send event locally, false: send result to primary
  const bool local;

  /// object range to scan: [begin, end)
  const hobject_t begin;
  const hobject_t end;

  /// result, see local
  ScrubMap ret;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_scan;

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter *) const final;

  seastar::future<> start();
  interruptible_future<> scan_object(const ghobject_t &obj);
  interruptible_future<> deep_scan_object(const ghobject_t &obj);

  ScrubScan(
    Ref<PG> pg, bool deep, bool local,
    const hobject_t &begin, const hobject_t &end);
  ~ScrubScan();
};

}

namespace crimson {

template <>
struct EventBackendRegistry<osd::ScrubRequested> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::ScrubMessage> {
  static std::tuple<> get_backends() {
    return {};
  }
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::ScrubRequested>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubMessage>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubScan>
  : fmt::ostream_formatter {};
#endif
