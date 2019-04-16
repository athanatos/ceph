// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <optional>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "common/dout.h"
#include "crimson/net/Fwd.h"
#include "os/Transaction.h"
#include "crimson/osd/shard_services.h"
#include "osd/osd_types.h"
#include "osd/osd_internal_types.h"
#include "osd/PeeringState.h"

template<typename T> using Ref = boost::intrusive_ptr<T>;
class OSDMap;
class MQuery;
class PGBackend;
class PGPeeringEvent;
namespace recovery {
  class Context;
}

namespace ceph::net {
  class Messenger;
}

namespace ceph::os {
  class CyanStore;
}

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>,
  PeeringState::PeeringListener,
  DoutPrefixProvider
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;

  spg_t pgid;
  pg_shard_t pg_whoami;
  coll_t coll;
  ceph::os::CyanStore::CollectionRef coll_ref;
  ghobject_t pgmeta_oid;
public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     pg_pool_t&& pool,
     std::string&& name,
     std::unique_ptr<PGBackend> backend,
     cached_map_t osdmap,
     ceph::osd::ShardServices &shard_services);

  // EpochSource
  epoch_t get_osdmap_epoch() const override final {
    return peering_state.get_osdmap_epoch();
  }

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override final {
    return out << *this;
  }
  CephContext *get_cct() const override final {
    return shard_services.get_cct();
  }
  unsigned get_subsys() const override final {
    return ceph_subsys_osd;
  }

  ceph::os::CyanStore::CollectionRef get_collection_ref() {
    return coll_ref;
  }

  // PeeringListener
  void prepare_write(
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ObjectStore::Transaction &t) override final {
    map<string,bufferlist> km;
    if (dirty_big_info || dirty_info) {
      int ret = prepare_info_keymap(
	shard_services.get_cct(),
	&km,
	peering_state.get_osdmap()->get_epoch(),
	info,
	last_written_info,
	past_intervals,
	dirty_big_info,
	need_write_epoch,
	true,
	nullptr,
	this);
      ceph_assert(ret == 0);
    }
    pglog.write_log_and_missing(
      t, &km, coll, pgmeta_oid,
      peering_state.get_pool().info.require_rollback());
    if (!km.empty())
      t.omap_setkeys(coll, pgmeta_oid, km);
  }

  void on_info_history_change() override final {
    // Not needed yet -- mainly for scrub scheduling
  }

  void scrub_requested(bool deep, bool repair) override final {
    ceph_assert(0 == "Not implemented");
  }

  uint64_t get_snap_trimq_size() const override final {
    return 0;
  }

  void send_cluster_message(
    int osd, Message *m,
    epoch_t epoch, bool share_map_update=false) override final {
    shard_services.send_to_osd(osd, m, epoch);
  }

  void send_pg_created(pg_t pgid) override final {
    shard_services.send_pg_created(pgid);
  }

  bool try_flush_or_schedule_async() override final {
    shard_services.get_store().do_transaction(
      coll_ref,
      ObjectStore::Transaction()).then(
	[this](){
	  PeeringState::PeeringCtx rctx;
	  auto evt = PeeringState::IntervalFlush();
	  do_peering_event(evt, rctx);
	  return shard_services.dispatch_context(std::move(rctx));
	});
    return false;
  }

  void start_flush_on_transaction(
    ObjectStore::Transaction &t) override final {
    t.register_on_commit(
      new LambdaContext([this](){
	peering_state.complete_flush();
    }));
  }

  void on_flushed() override final {
    // will be needed for unblocking IO operations/peering
  }


  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void update_local_background_io_priority(
    unsigned priority) override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void cancel_local_background_io_reservation() override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void cancel_remote_recovery_reservation() override final {
    ceph_assert(0 == "Not implemented yet");
  }

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) override final {
    t.register_on_commit(
      new LambdaContext([this, on_commit](){
	PeeringState::PeeringCtx rctx;
        do_peering_event(on_commit, rctx);
	shard_services.dispatch_context(std::move(rctx));
    }));
  }

  void update_heartbeat_peers(set<int> peers) override final {
    // Not needed yet
  }
  void set_probe_targets(const set<pg_shard_t> &probe_set) override final {
    // Not needed yet
  }
  void clear_probe_targets() override final {
    // Not needed yet
  }
  void queue_want_pg_temp(const vector<int> &wanted) override final {
    shard_services.queue_want_pg_temp(pgid.pgid, wanted);
  }
  void clear_want_pg_temp() override final {
    shard_services.remove_want_pg_temp(pgid.pgid);
  }
  void publish_stats_to_osd() override final {
    // Not needed yet
  }
  void clear_publish_stats() override final {
    // Not needed yet
  }
  void check_recovery_sources(const OSDMapRef& newmap) override final {
    // Not needed yet
  }
  void check_blacklisted_watchers() override final {
    // Not needed yet
  }
  void clear_primary_state() override final {
    // Not needed yet
  }


  void on_pool_change() override final {
    // Not needed yet
  }
  void on_role_change() override final {
    // Not needed yet
  }
  void on_change(ObjectStore::Transaction &t) override final {
    // Not needed yet
  }
  void on_activate(interval_set<snapid_t> to_trim) override final {
    // Not needed yet (will be needed for IO unblocking)
  }
  void on_activate_complete() override final {
    active_promise.set_value();
    active_promise = {};
  }
  void on_new_interval() override final {
    // Not needed yet
  }
  Context *on_clean() override final {
    // Not needed yet (will be needed for IO unblocking)
    return nullptr;
  }
  void on_activate_committed() override final {
    // Not needed yet (will be needed for IO unblocking)
  }
  void on_active_exit() override final {
    // Not needed yet
  }

  void on_removal(ObjectStore::Transaction &t) override final {
    // TODO
  }
  void do_delete_work(ObjectStore::Transaction &t) override final {
    // TODO
  }


  // merge/split not ready
  void clear_ready_to_merge() override final {}
  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) override final {}
  void set_not_ready_to_merge_source(pg_t pgid) override final {}
  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) override final {}
  void set_ready_to_merge_source(eversion_t lu) override final {}

  void on_active_actmap() override final {
    // Not needed yet
  }
  void on_active_advmap(const OSDMapRef &osdmap) override final {
    // Not needed yet
  }
  epoch_t oldest_stored_osdmap() override final {
    // TODO
    return 0;
  }


  void on_backfill_reserved() override final {
    ceph_assert(0 == "Not implemented");
  }
  void on_backfill_canceled() override final {
    ceph_assert(0 == "Not implemented");
  }
  void on_recovery_reserved() override final {
    ceph_assert(0 == "Not implemented");
  }


  bool try_reserve_recovery_space(
    int64_t primary_num_bytes, int64_t local_num_bytes) override final {
    return true;
  }
  void unreserve_recovery_space() override final {}

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ObjectStore::Transaction *t;
    PGLogEntryHandler(PG *pg, ObjectStore::Transaction *t) : pg(pg), t(t) {}

    // LogEntryHandler
    void remove(const hobject_t &hoid) override {
      // TODO
    }
    void try_stash(const hobject_t &hoid, version_t v) override {
      // TODO
    }
    void rollback(const pg_log_entry_t &entry) override {
      // TODO
    }
    void rollforward(const pg_log_entry_t &entry) override {
      // TODO
    }
    void trim(const pg_log_entry_t &entry) override {
      // TODO
    }
  };
  PGLog::LogEntryHandlerRef get_log_handler(
    ObjectStore::Transaction &t) override final {
    return std::make_unique<PG::PGLogEntryHandler>(this, &t);
  }

  void rebuild_missing_set_with_deletes(PGLog &pglog) override final {
    ceph_assert(0 == "Impossible for crimson");
  }

  PerfCounters &get_peering_perf() override final {
    return shard_services.get_recoverystate_perf_logger();
  }
  PerfCounters &get_perf_logger() override final {
    return shard_services.get_perf_logger();
  }

  void log_state_enter(const char *state) override final;
  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) override final;

  void dump_recovery_info(Formatter *f) const override final {
  }

  OstreamTemp get_clog_info() override final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_INFO, nullptr);
  }
  OstreamTemp get_clog_debug() override final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_DEBUG, nullptr);
  }
  OstreamTemp get_clog_error() override final {
    // not needed yet: replace with not a stub (needs to be wired up to monc)
    return OstreamTemp(CLOG_ERROR, nullptr);
  }


  // Utility
  bool is_primary() const {
    return peering_state.is_primary();
  }
  pg_stat_t get_stats() {
    auto stats = peering_state.prepare_stats_for_publish(
      false,
      pg_stat_t(),
      object_stat_collection_t());
    ceph_assert(stats);
    return *stats;
  }
  bool get_need_up_thru() const {
    return peering_state.get_need_up_thru();
  }


  /// initialize created PG
  void init(
    ceph::os::CyanStore::CollectionRef coll_ref,
    int role,
    const vector<int>& up,
    int up_primary,
    const vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    bool backfill,
    ObjectStore::Transaction &t);

  seastar::future<> read_state(ceph::os::CyanStore* store);

  void do_peering_event(
    const boost::statechart::event_base &evt,
    PeeringState::PeeringCtx &rctx);
  void do_peering_event(
    PGPeeringEvent& evt, PeeringState::PeeringCtx &rctx) {
    return do_peering_event(evt.get_event(), rctx);
  }
  void do_peering_event(
    std::unique_ptr<PGPeeringEvent> evt,
    PeeringState::PeeringCtx &rctx) {
    return do_peering_event(*evt, rctx);
  }
  void do_peering_event(
    PGPeeringEventRef evt,
    PeeringState::PeeringCtx &rctx) {
    return do_peering_event(*evt, rctx);
  }

#if 0
  template<typename T>
  void do_peering_event(
    const T &evt,
    PeeringState::PeeringCtx &rctx) {
    return do_peering_event(
      std::make_unique<PGPeeringEvent>(
	peering_state.get_osdmap()->get_epoch(),
	peering_state.get_osdmap()->get_epoch(),
	evt),
      rctx);
  }
#endif

  void handle_advance_map(cached_map_t next_map, PeeringState::PeeringCtx &rctx);
  void handle_activate_map(PeeringState::PeeringCtx &rctx);
  void handle_initialize(PeeringState::PeeringCtx &rctx);
  seastar::future<> handle_op(ceph::net::Connection* conn,
			      Ref<MOSDOp> m);
  void print(ostream& os) const;
private:

  seastar::future<Ref<MOSDOpReply>> do_osd_ops(Ref<MOSDOp> m);
  seastar::future<> do_osd_op(
    ObjectState& os,
    OSDOp& op,
    ceph::os::Transaction& txn);

private:
  ceph::osd::ShardServices &shard_services;

  cached_map_t osdmap;
  std::unique_ptr<PGBackend> backend;

  PeeringState peering_state;

  seastar::shared_promise<> active_promise;
  seastar::future<> wait_for_active();

  friend std::ostream& operator<<(std::ostream&, const PG& pg);
};

std::ostream& operator<<(std::ostream&, const PG& pg);
