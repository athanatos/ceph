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
#include "osd/osd_types.h"
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

public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     pg_pool_t&& pool,
     std::string&& name,
     std::unique_ptr<PGBackend> backend,
     cached_map_t osdmap,
     ceph::net::Messenger& msgr);
  // EpochSource
  epoch_t get_osdmap_epoch() const override final {
    return peering_state.get_osdmap_epoch();
  }

  // DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override final {
    return out << *this;
  }
  CephContext *get_cct() const override final {
    return nullptr; // TODO
  }
  unsigned get_subsys() const override final {
    return ceph_subsys_osd;
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
    ObjectStore::Transaction &t) override final {}

    
  void on_info_history_change() override final {}
    
  void scrub_requested(bool deep, bool repair) override final {}

    
  uint64_t get_snap_trimq_size() const override final {
    return 0;
  }
    
  void send_cluster_message(
    int osd, Message *m,
    epoch_t epoch, bool share_map_update=false) override final {
  }
    
  void send_pg_created(pg_t pgid) override final {}

  bool try_flush_or_schedule_async() override final {
    return true;
  }
    
  void start_flush_on_transaction(
    ObjectStore::Transaction *t) override final {}
    
  void on_flushed() override final {}

    
  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) override final {}

  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) override final {}
    
  void update_local_background_io_priority(
    unsigned priority) override final {}
    
  void cancel_local_background_io_reservation() override final {}

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventRef on_grant,
    PGPeeringEventRef on_preempt) override final {}
    
  void cancel_remote_recovery_reservation() override final {}

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) override final {}

  void update_heartbeat_peers(set<int> peers) override final {}
  void set_probe_targets(const set<pg_shard_t> &probe_set) override final {}
  void clear_probe_targets() override final {}
  void queue_want_pg_temp(const vector<int> &wanted) override final {}
  void clear_want_pg_temp() override final {}
  void publish_stats_to_osd() override final {}
  void clear_publish_stats() override final {}
  void check_recovery_sources(const OSDMapRef& newmap) override final {}
  void check_blacklisted_watchers() override final {}
  void clear_primary_state() override final {}

    
  void on_pool_change() override final {}
  void on_role_change() override final {}
  void on_change(ObjectStore::Transaction *t) override final {}
  void on_activate(interval_set<snapid_t> to_trim) override final {}
  void on_activate_complete() override final {}
  void on_new_interval() override final {}
  Context *on_clean() override final {
    return nullptr;
  }
  void on_activate_committed() override final {}
  void on_active_exit() override final {}
    
  void on_removal(ObjectStore::Transaction *t) override final {}
  void do_delete_work(ObjectStore::Transaction *t) override final {}

    
  void clear_ready_to_merge() override final {}
  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) override final {}
  void set_not_ready_to_merge_source(pg_t pgid) override final {}
  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) override final {}
  void set_ready_to_merge_source(eversion_t lu) override final {}

    
  void on_active_actmap() override final {}
  void on_active_advmap(const OSDMapRef &osdmap) override final {}
  epoch_t oldest_stored_osdmap() override final {
    return 0;
  }

    
  void on_backfill_reserved() override final {}
  void on_backfill_canceled() override final {}
  void on_recovery_reserved() override final {}

    
  bool try_reserve_recovery_space(
    int64_t primary_num_bytes, int64_t local_num_bytes) override final {
    return true;
  }
  void unreserve_recovery_space() override final {}

  PGLog::LogEntryHandlerRef get_log_handler(
    ObjectStore::Transaction *t) override final {
    return nullptr;
  }
    
  void rebuild_missing_set_with_deletes(PGLog &pglog) override final {}

    
  PerfCounters &get_peering_perf() override final {
    return *((PerfCounters*)nullptr);
  }
  PerfCounters &get_perf_logger() override final {
    return *((PerfCounters*)nullptr);
  }
  void log_state_enter(const char *state) override final {}
  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) override final {}
  void dump_recovery_info(Formatter *f) const override final {}
  LogChannel &get_clog() override final {
    return *((LogChannel*)nullptr);
  }


  // Utility
  bool is_primary() const {
    return peering_state.is_primary();
  }
  pg_stat_t get_stats() const {
    return peering_state.get_info().stats;
  }
  bool get_need_up_thru() const {
    return peering_state.get_need_up_thru();
  }

  seastar::future<> read_state(ceph::os::CyanStore* store);

  // peering/recovery
  void on_activated();
  void maybe_mark_clean();
  void reply_pg_query_for_log(const MQuery& query, bool full);
  seastar::future<> send_to_osd(int peer, Ref<Message> m, epoch_t from_epoch);

  seastar::future<> do_peering_event(std::unique_ptr<PGPeeringEvent> evt);
  seastar::future<> dispatch_context(PeeringState::PeeringCtx &&ctx);
  seastar::future<> handle_advance_map(cached_map_t next_map);
  seastar::future<> handle_activate_map();
  seastar::future<> handle_op(ceph::net::ConnectionRef conn,
			      Ref<MOSDOp> m);
  void print(ostream& os) const;
private:

  seastar::future<Ref<MOSDOpReply>> do_osd_ops(Ref<MOSDOp> m);
  seastar::future<> do_osd_op(const object_info_t& oi, OSDOp* op);

private:
  PeeringState peering_state;

  std::optional<seastar::shared_promise<>> active_promise;
  seastar::future<> wait_for_active();

  std::unique_ptr<PGBackend> backend;

  cached_map_t osdmap;
  ceph::net::Messenger& msgr;

  friend std::ostream& operator<<(std::ostream&, const PG& pg);
};

std::ostream& operator<<(std::ostream&, const PG& pg);
