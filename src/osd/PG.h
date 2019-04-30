// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/container/flat_set.hpp>
#include "include/mempool.h"

// re-include our assert to clobber boost's
#include "include/ceph_assert.h" 

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/xlist.h"
#include "SnapMapper.h"
#include "Session.h"
#include "common/Timer.h"

#include "PGLog.h"
#include "OSDMap.h"
#include "messages/MOSDPGLog.h"
#include "include/str_list.h"
#include "PGBackend.h"
#include "PGPeeringEvent.h"
#include "PeeringState.h"
#include "MissingLoc.h"

#include "mgr/OSDPerfMetricTypes.h"

#include <atomic>
#include <list>
#include <memory>
#include <string>
#include <tuple>

//#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs
//#define PG_DEBUG_REFS    // track provenance of pg refs, helpful for finding leaks

class OSD;
class OSDService;
class OSDShard;
class OSDShardPGSlot;
class MOSDOp;
class MOSDPGScan;
class MOSDPGBackfill;
class MOSDPGInfo;

class PG;
struct OpRequest;
typedef OpRequest::Ref OpRequestRef;
class MOSDPGLog;
class CephContext;
class DynamicPerfStats;

namespace Scrub {
  class Store;
}

#ifdef PG_DEBUG_REFS
#include "common/tracked_int_ptr.hpp"
  uint64_t get_with_id(PG *pg);
  void put_with_id(PG *pg, uint64_t id);
  typedef TrackedIntPtr<PG> PGRef;
#else
  typedef boost::intrusive_ptr<PG> PGRef;
#endif

class PGRecoveryStats {
  struct per_state_info {
    uint64_t enter, exit;     // enter/exit counts
    uint64_t events;
    utime_t event_time;       // time spent processing events
    utime_t total_time;       // total time in state
    utime_t min_time, max_time;

    // cppcheck-suppress unreachableCode
    per_state_info() : enter(0), exit(0), events(0) {}
  };
  map<const char *,per_state_info> info;
  Mutex lock;

  public:
  PGRecoveryStats() : lock("PGRecoverStats::lock") {}

  void reset() {
    std::lock_guard l(lock);
    info.clear();
  }
  void dump(ostream& out) {
    std::lock_guard l(lock);
    for (map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); ++p) {
      per_state_info& i = p->second;
      out << i.enter << "\t" << i.exit << "\t"
	  << i.events << "\t" << i.event_time << "\t"
	  << i.total_time << "\t"
	  << i.min_time << "\t" << i.max_time << "\t"
	  << p->first << "\n";
    }
  }

  void dump_formatted(Formatter *f) {
    std::lock_guard l(lock);
    f->open_array_section("pg_recovery_stats");
    for (map<const char *,per_state_info>::iterator p = info.begin();
	 p != info.end(); ++p) {
      per_state_info& i = p->second;
      f->open_object_section("recovery_state");
      f->dump_int("enter", i.enter);
      f->dump_int("exit", i.exit);
      f->dump_int("events", i.events);
      f->dump_stream("event_time") << i.event_time;
      f->dump_stream("total_time") << i.total_time;
      f->dump_stream("min_time") << i.min_time;
      f->dump_stream("max_time") << i.max_time;
      vector<string> states;
      get_str_vec(p->first, "/", states);
      f->open_array_section("nested_states");
      for (vector<string>::iterator st = states.begin();
	   st != states.end(); ++st) {
	f->dump_string("state", *st);
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
  }

  void log_enter(const char *s) {
    std::lock_guard l(lock);
    info[s].enter++;
  }
  void log_exit(const char *s, utime_t dur, uint64_t events, utime_t event_dur) {
    std::lock_guard l(lock);
    per_state_info &i = info[s];
    i.exit++;
    i.total_time += dur;
    if (dur > i.max_time)
      i.max_time = dur;
    if (dur < i.min_time || i.min_time == utime_t())
      i.min_time = dur;
    i.events += events;
    i.event_time += event_dur;
  }
};

/** PG - Replica Placement Group
 *
 */

class PG : public DoutPrefixProvider, public PeeringState::PeeringListener {
  friend class NamedState;
  friend class PeeringState;

protected:
  PeeringState recovery_state;
public:
  using PeeringCtx = PeeringState::PeeringCtx;


protected:
  /**
   * Peering state information being moved to PeeringState
   */
  int &role;
  uint64_t &state;
  pg_shard_t &primary;
  pg_shard_t &pg_whoami;
  pg_shard_t &up_primary;
  vector<int> &up;
  set<pg_shard_t> &upset;
  vector<int> &acting;
  set<pg_shard_t> &actingset;
  set<pg_shard_t> &acting_recovery_backfill;
  bool &send_notify;
  bool &dirty_info;
  bool &dirty_big_info;
  pg_info_t &info;
  pg_info_t &last_written_info;
  PastIntervals &past_intervals;
  PGLog &pg_log;
  epoch_t &last_peering_reset;
  eversion_t &last_update_ondisk;
  eversion_t &last_complete_ondisk;
  eversion_t &last_update_applied;
  eversion_t &last_rollback_info_trimmed_to_applied;
  set<pg_shard_t> &stray_set;
  map<pg_shard_t, pg_info_t> &peer_info;
  map<pg_shard_t, int64_t> &peer_bytes;
  set<pg_shard_t> &peer_purged;
  map<pg_shard_t, pg_missing_t> &peer_missing;
  set<pg_shard_t> &peer_log_requested;
  set<pg_shard_t> &peer_missing_requested;
  uint64_t &peer_features;
  uint64_t &acting_features;
  uint64_t &upacting_features;
  unsigned &last_require_osd_release;
  vector<int> &want_acting;
  map<pg_shard_t,eversion_t> &peer_last_complete_ondisk;
  eversion_t &min_last_complete_ondisk;
  eversion_t &pg_trim_to;
  set<int> &blocked_by;
  set<pg_shard_t> &peer_activated;
  set<pg_shard_t> &backfill_targets;
  set<pg_shard_t> &async_recovery_targets;
  set<pg_shard_t> &might_have_unfound;
  bool &deleting;
  atomic<bool> &deleted;
  MissingLoc &missing_loc;

public:
  // -- members --
  const spg_t pg_id;
  const coll_t coll;

  ObjectStore::CollectionHandle ch;

  // -- methods --
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext *get_cct() const override {
    return cct;
  }
  unsigned get_subsys() const override {
    return ceph_subsys_osd;
  }

  const OSDMapRef& get_osdmap() const {
    ceph_assert(is_locked());
    return recovery_state.get_osdmap();
  }

  epoch_t get_osdmap_epoch() const override {
    return recovery_state.get_osdmap()->get_epoch();
  }

  PerfCounters &get_peering_perf() override;

  void lock_suspend_timeout(ThreadPool::TPHandle &handle) {
    handle.suspend_tp_timeout();
    lock();
    handle.reset_tp_timeout();
  }
  void lock(bool no_lockdep = false) const;
  void unlock() const {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    ceph_assert(!dirty_info);
    ceph_assert(!dirty_big_info);
    _lock.Unlock();
  }
  bool is_locked() const {
    return _lock.is_locked();
  }

  const spg_t& get_pgid() const {
    return pg_id;
  }

  const PGPool& get_pool() const {
    return pool;
  }
  uint64_t get_last_user_version() const {
    return info.last_user_version;
  }
  const pg_history_t& get_history() const {
    return info.history;
  }
  bool get_need_up_thru() const {
    return need_up_thru;
  }
  epoch_t get_same_interval_since() const {
    return info.history.same_interval_since;
  }

  void set_last_scrub_stamp(utime_t t) {
    info.stats.last_scrub_stamp = t;
    info.history.last_scrub_stamp = t;
  }

  void set_last_deep_scrub_stamp(utime_t t) {
    info.stats.last_deep_scrub_stamp = t;
    info.history.last_deep_scrub_stamp = t;
  }

  bool is_deleting() const {
    return recovery_state.is_deleting();
  }
  bool is_deleted() const {
    return recovery_state.is_deleted();
  }
  bool is_replica() const {
    return recovery_state.is_replica();
  }
  bool is_primary() const {
    return recovery_state.is_primary();
  }
  bool pg_has_reset_since(epoch_t e) {
    ceph_assert(is_locked());
    return recovery_state.pg_has_reset_since(e);
  }

  bool is_ec_pg() const {
    return recovery_state.is_ec_pg();
  }
  int get_role() const {
    return recovery_state.get_role();
  }
  const vector<int> get_acting() const {
    return recovery_state.get_acting();
  }
  int get_acting_primary() const {
    return recovery_state.get_acting_primary();
  }
  pg_shard_t get_primary() const {
    return recovery_state.get_primary();
  }
  const vector<int> get_up() const {
    return recovery_state.get_up();
  }
  int get_up_primary() const {
    return recovery_state.get_up_primary();
  }
  const PastIntervals& get_past_intervals() const {
    return recovery_state.past_intervals;
  }
  bool is_acting_recovery_backfill(pg_shard_t osd) const {
    return recovery_state.is_acting_recovery_backfill(osd);
  }
  bool is_acting(pg_shard_t osd) const {
    return recovery_state.is_acting(osd);
  }
  bool is_up(pg_shard_t osd) const {
    return recovery_state.is_up(osd);
  }
  static bool has_shard(bool ec, const vector<int>& v, pg_shard_t osd) {
    return PeeringState::has_shard(ec, v, osd);
  }

  /// initialize created PG
  void init(
    int role,
    const vector<int>& up,
    int up_primary,
    const vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    bool backfill,
    ObjectStore::Transaction *t);

  /// read existing pg state off disk
  void read_state(ObjectStore *store);
  static int peek_map_epoch(ObjectStore *store, spg_t pgid, epoch_t *pepoch);

  static int get_latest_struct_v() {
    return latest_struct_v;
  }
  static int get_compat_struct_v() {
    return compat_struct_v;
  }
  static int read_info(
    ObjectStore *store, spg_t pgid, const coll_t &coll,
    pg_info_t &info, PastIntervals &past_intervals,
    __u8 &);
  static bool _has_removal_flag(ObjectStore *store, spg_t pgid);

  void rm_backoff(BackoffRef b);

  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
  void start_split_stats(const set<spg_t>& childpgs, vector<object_stat_sum_t> *v);
  virtual void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction *t) = 0;
  void split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void merge_from(map<spg_t,PGRef>& sources, PeeringCtx *rctx,
		  unsigned split_bits,
		  const pg_merge_meta_t& last_pg_merge_meta);
  void finish_split_stats(const object_stat_sum_t& stats, ObjectStore::Transaction *t);

  void scrub(epoch_t queued, ThreadPool::TPHandle &handle);

  void reg_next_scrub() override;
  void unreg_next_scrub() override;

  bool is_forced_recovery_or_backfill() const {
    return get_state() & (PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);
  }
  bool set_force_recovery(bool b);
  bool set_force_backfill(bool b);

  void queue_peering_event(PGPeeringEventRef evt);
  void do_peering_event(PGPeeringEventRef evt, PeeringCtx *rcx);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(
    OSDMapRef osdmap, OSDMapRef lastmap,
    vector<int>& newup, int up_primary,
    vector<int>& newacting, int acting_primary,
    PeeringCtx *rctx);
  void handle_activate_map(PeeringCtx *rctx);
  void handle_initialize(PeeringCtx *rctx);
  void handle_query_state(Formatter *f);

  /**
   * @param ops_begun returns how many recovery ops the function started
   * @returns true if any useful work was accomplished; false otherwise
   */
  virtual bool start_recovery_ops(
    uint64_t max,
    ThreadPool::TPHandle &handle,
    uint64_t *ops_begun) = 0;

  // more work after the above, but with a PeeringCtx
  void find_unfound(epoch_t queued, PeeringCtx *rctx);

  virtual void get_watchers(std::list<obj_watch_item_t> *ls) = 0;

  void dump_pgstate_history(Formatter *f);
  void dump_missing(Formatter *f);

  void get_pg_stats(std::function<void(const pg_stat_t&, epoch_t lec)> f);
  void with_heartbeat_peers(std::function<void(int)> f);

  void shutdown();
  virtual void on_shutdown() = 0;

  bool get_must_scrub() const {
    return scrubber.must_scrub;
  }
  bool sched_scrub();

  virtual void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle
  ) = 0;
  virtual void clear_cache() = 0;
  virtual int get_cache_obj_count() = 0;

  virtual void snap_trimmer(epoch_t epoch_queued) = 0;
  virtual int do_command(
    cmdmap_t cmdmap,
    ostream& ss,
    bufferlist& idata,
    bufferlist& odata,
    ConnectionRef conn,
    ceph_tid_t tid) = 0;

  virtual bool agent_work(int max) = 0;
  virtual bool agent_work(int max, int agent_flush_quota) = 0;
  virtual void agent_stop() = 0;
  virtual void agent_delay() = 0;
  virtual void agent_clear() = 0;
  virtual void agent_choose_mode_restart() = 0;

  virtual void on_removal(ObjectStore::Transaction *t) = 0;

  struct C_DeleteMore : public Context {
    PGRef pg;
    epoch_t epoch;
    C_DeleteMore(PG *p, epoch_t e) : pg(p), epoch(e) {}
    void finish(int r) override {
      ceph_abort();
    }
    void complete(int r) override;
  };

  void _delete_some(ObjectStore::Transaction *t);

  virtual void set_dynamic_perf_stats_queries(
    const std::list<OSDPerfMetricQuery> &queries) {
  }
  virtual void get_dynamic_perf_stats(DynamicPerfStats *stats) {
  }

  // reference counting
#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  void get(const char* tag);
  void put(const char* tag);
  int get_num_ref() {
    return ref;
  }

  // ctor
  PG(OSDService *o, OSDMapRef curmap,
     const PGPool &pool, spg_t p);
  ~PG() override;

  // prevent copying
  explicit PG(const PG& rhs) = delete;
  PG& operator=(const PG& rhs) = delete;

protected:
  // -------------
  // protected
  OSDService *osd;
public:
  OSDShard *osd_shard = nullptr;
  OSDShardPGSlot *pg_slot = nullptr;
protected:
  CephContext *cct;

  const PGPool &pool;

  // locking and reference counting.
  // I destroy myself when the reference count hits zero.
  // lock() should be called before doing anything.
  // get() should be called on pointer copy (to another thread, etc.).
  // put() should be called on destruction of some previously copied pointer.
  // unlock() when done with the current pointer (_most common_).
  mutable Mutex _lock = {"PG::_lock"};

  std::atomic<unsigned int> ref{0};

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock = {"PG::_ref_id_lock"};
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id = 0;

  friend uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  friend void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

private:
  friend void intrusive_ptr_add_ref(PG *pg) {
    pg->get("intptr");
  }
  friend void intrusive_ptr_release(PG *pg) {
    pg->put("intptr");
  }


  // =====================

protected:
  OSDriver osdriver;
  SnapMapper snap_mapper;
  bool eio_errors_to_process = false;

  virtual PGBackend *get_pgbackend() = 0;
  virtual const PGBackend* get_pgbackend() const = 0;

protected:
  /*** PG ****/
  /// get_is_recoverable_predicate: caller owns returned pointer and must delete when done
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const {
    return get_pgbackend()->get_is_recoverable_predicate();
  }
protected:
  void requeue_map_waiters();

protected:

  ZTracer::Endpoint trace_endpoint;


protected:
  __u8 info_struct_v = 0;
  static const __u8 latest_struct_v = 10;
  // v10 is the new past_intervals encoding
  // v9 was fastinfo_key addition
  // v8 was the move to a per-pg pgmeta object
  // v7 was SnapMapper addition in 86658392516d5175b2756659ef7ffaaf95b0f8ad
  // (first appeared in cuttlefish).
  static const __u8 compat_struct_v = 10;
  void upgrade(ObjectStore *store);

protected:
  ghobject_t    pgmeta_oid;

  // ------------------
  interval_set<snapid_t> snap_trimq;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item stat_queue_item;
  bool scrub_queued;
  bool recovery_queued;

  int recovery_ops_active;
  set<pg_shard_t> waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  multiset<hobject_t> recovering_oids;
#endif

public:
  bool dne() { return info.dne(); }

  virtual void send_cluster_message(int osd, Message *m, epoch_t epoch);

protected:
  bool need_up_thru; ///< Flag indicating that this pg needs up through published

  epoch_t get_last_peering_reset() const {
    return last_peering_reset;
  }

  /* heartbeat peers */
  void set_probe_targets(const set<pg_shard_t> &probe_set);
  void clear_probe_targets();

  Mutex heartbeat_peer_lock;
  set<int> heartbeat_peers;
  set<int> probe_targets;

public:
  /**
   * BackfillInterval
   *
   * Represents the objects in a range [begin, end)
   *
   * Possible states:
   * 1) begin == end == hobject_t() indicates the the interval is unpopulated
   * 2) Else, objects contains all objects in [begin, end)
   */
  struct BackfillInterval {
    // info about a backfill interval on a peer
    eversion_t version; /// version at which the scan occurred
    map<hobject_t,eversion_t> objects;
    hobject_t begin;
    hobject_t end;

    /// clear content
    void clear() {
      *this = BackfillInterval();
    }

    /// clear objects list only
    void clear_objects() {
      objects.clear();
    }

    /// reinstantiate with a new start+end position and sort order
    void reset(hobject_t start) {
      clear();
      begin = end = start;
    }

    /// true if there are no objects in this interval
    bool empty() const {
      return objects.empty();
    }

    /// true if interval extends to the end of the range
    bool extends_to_end() const {
      return end.is_max();
    }

    /// removes items <= soid and adjusts begin to the first object
    void trim_to(const hobject_t &soid) {
      trim();
      while (!objects.empty() &&
	     objects.begin()->first <= soid) {
	pop_front();
      }
    }

    /// Adjusts begin to the first object
    void trim() {
      if (!objects.empty())
	begin = objects.begin()->first;
      else
	begin = end;
    }

    /// drop first entry, and adjust @begin accordingly
    void pop_front() {
      ceph_assert(!objects.empty());
      objects.erase(objects.begin());
      trim();
    }

    /// dump
    void dump(Formatter *f) const {
      f->dump_stream("begin") << begin;
      f->dump_stream("end") << end;
      f->open_array_section("objects");
      for (map<hobject_t, eversion_t>::const_iterator i =
	     objects.begin();
	   i != objects.end();
	   ++i) {
	f->open_object_section("object");
	f->dump_stream("object") << i->first;
	f->dump_stream("version") << i->second;
	f->close_section();
      }
      f->close_section();
    }
  };

protected:
  BackfillInterval backfill_info;
  map<pg_shard_t, BackfillInterval> peer_backfill_info;
  bool backfill_reserved;
  bool backfill_reserving;

  // The primary's num_bytes and local num_bytes for this pg, only valid
  // during backfill for non-primary shards.
  // Both of these are adjusted for EC to reflect the on-disk bytes
  std::atomic<int64_t> primary_num_bytes = 0;
  std::atomic<int64_t> local_num_bytes = 0;

public:
  bool is_backfill_targets(pg_shard_t osd) {
    return backfill_targets.count(osd);
  }

  // Space reserved for backfill is primary_num_bytes - local_num_bytes
  // Don't care that difference itself isn't atomic
  uint64_t get_reserved_num_bytes() {
    int64_t primary = primary_num_bytes.load();
    int64_t local = local_num_bytes.load();
    if (primary > local)
      return primary - local;
    else
      return 0;
  }

  bool is_remote_backfilling() {
    return primary_num_bytes.load() > 0;
  }

  void set_reserved_num_bytes(int64_t primary, int64_t local);
  void clear_reserved_num_bytes();

  // If num_bytes are inconsistent and local_num- goes negative
  // it's ok, because it would then be ignored.

  // The value of num_bytes could be negative,
  // but we don't let local_num_bytes go negative.
  void add_local_num_bytes(int64_t num_bytes) {
    if (num_bytes) {
      int64_t prev_bytes = local_num_bytes.load();
      int64_t new_bytes;
      do {
        new_bytes = prev_bytes + num_bytes;
        if (new_bytes < 0)
          new_bytes = 0;
      } while(!local_num_bytes.compare_exchange_weak(prev_bytes, new_bytes));
    }
  }
  void sub_local_num_bytes(int64_t num_bytes) {
    ceph_assert(num_bytes >= 0);
    if (num_bytes) {
      int64_t prev_bytes = local_num_bytes.load();
      int64_t new_bytes;
      do {
        new_bytes = prev_bytes - num_bytes;
        if (new_bytes < 0)
          new_bytes = 0;
      } while(!local_num_bytes.compare_exchange_weak(prev_bytes, new_bytes));
    }
  }
  // The value of num_bytes could be negative,
  // but we don't let info.stats.stats.sum.num_bytes go negative.
  void add_num_bytes(int64_t num_bytes) {
    ceph_assert(_lock.is_locked_by_me());
    if (num_bytes) {
      info.stats.stats.sum.num_bytes += num_bytes;
      if (info.stats.stats.sum.num_bytes < 0) {
        info.stats.stats.sum.num_bytes = 0;
      }
    }
  }
  void sub_num_bytes(int64_t num_bytes) {
    ceph_assert(_lock.is_locked_by_me());
    ceph_assert(num_bytes >= 0);
    if (num_bytes) {
      info.stats.stats.sum.num_bytes -= num_bytes;
      if (info.stats.stats.sum.num_bytes < 0) {
        info.stats.stats.sum.num_bytes = 0;
      }
    }
  }

  // Only used in testing so not worried about needing the PG lock here
  int64_t get_stats_num_bytes() {
    Mutex::Locker l(_lock);
    int num_bytes = info.stats.stats.sum.num_bytes;
    if (pool.info.is_erasure()) {
      num_bytes /= (int)get_pgbackend()->get_ec_data_chunk_count();
      // Round up each object by a stripe
      num_bytes +=  get_pgbackend()->get_ec_stripe_chunk_size() * info.stats.stats.sum.num_objects;
    }
    int64_t lnb = local_num_bytes.load();
    if (lnb && lnb != num_bytes) {
      lgeneric_dout(cct, 0) << this << " " << info.pgid << " num_bytes mismatch "
			    << lnb << " vs stats "
                            << info.stats.stats.sum.num_bytes << " / chunk "
                            << get_pgbackend()->get_ec_data_chunk_count()
                            << dendl;
    }
    return num_bytes;
  }

protected:

  /*
   * blocked request wait hierarchy
   *
   * In order to preserve request ordering we need to be careful about the
   * order in which blocked requests get requeued.  Generally speaking, we
   * push the requests back up to the op_wq in reverse order (most recent
   * request first) so that they come back out again in the original order.
   * However, because there are multiple wait queues, we need to requeue
   * waitlists in order.  Generally speaking, we requeue the wait lists
   * that are checked first.
   *
   * Here are the various wait lists, in the order they are used during
   * request processing, with notes:
   *
   *  - waiting_for_map
   *    - may start or stop blocking at any time (depending on client epoch)
   *  - waiting_for_peered
   *    - !is_peered()
   *    - only starts blocking on interval change; never restarts
   *  - waiting_for_flush
   *    - flushes_in_progress
   *    - waiting for final flush during activate
   *  - waiting_for_active
   *    - !is_active()
   *    - only starts blocking on interval change; never restarts
   *  - waiting_for_scrub
   *    - starts and stops blocking for varying intervals during scrub
   *  - waiting_for_unreadable_object
   *    - never restarts once object is readable (* except for EIO?)
   *  - waiting_for_degraded_object
   *    - never restarts once object is writeable (* except for EIO?)
   *  - waiting_for_blocked_object
   *    - starts and stops based on proxied op activity
   *  - obc rwlocks
   *    - starts and stops based on read/write activity
   *
   * Notes:
   *
   *  1. During and interval change, we requeue *everything* in the above order.
   *
   *  2. When an obc rwlock is released, we check for a scrub block and requeue
   *     the op there if it applies.  We ignore the unreadable/degraded/blocked
   *     queues because we assume they cannot apply at that time (this is
   *     probably mostly true).
   *
   *  3. The requeue_ops helper will push ops onto the waiting_for_map list if
   *     it is non-empty.
   *
   * These three behaviors are generally sufficient to maintain ordering, with
   * the possible exception of cases where we make an object degraded or
   * unreadable that was previously okay, e.g. when scrub or op processing
   * encounter an unexpected error.  FIXME.
   */

  // ops with newer maps than our (or blocked behind them)
  // track these by client, since inter-request ordering doesn't otherwise
  // matter.
  unordered_map<entity_name_t,list<OpRequestRef>> waiting_for_map;

  // ops waiting on peered
  list<OpRequestRef>            waiting_for_peered;

  // ops waiting on active (require peered as well)
  list<OpRequestRef>            waiting_for_active;
  list<OpRequestRef>            waiting_for_flush;
  list<OpRequestRef>            waiting_for_scrub;

  list<OpRequestRef>            waiting_for_cache_not_full;
  list<OpRequestRef>            waiting_for_clean_to_primary_repair;
  map<hobject_t, list<OpRequestRef>> waiting_for_unreadable_object,
			     waiting_for_degraded_object,
			     waiting_for_blocked_object;

  set<hobject_t> objects_blocked_on_cache_full;
  map<hobject_t,snapid_t> objects_blocked_on_degraded_snap;
  map<hobject_t,ObjectContextRef> objects_blocked_on_snap_promotion;

  // Callbacks should assume pg (and nothing else) is locked
  map<hobject_t, list<Context*>> callbacks_for_degraded_object;

  map<eversion_t,
      list<tuple<OpRequestRef, version_t, int> > > waiting_for_ondisk;

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef>>& m);
  void requeue_op(OpRequestRef op);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  void _update_calc_stats();
  void _update_blocked_by();
  friend class TestOpsSocketHook;
  void publish_stats_to_osd();
  void clear_publish_stats();

  void clear_primary_state();

  bool needs_recovery() const;
  bool needs_backfill() const;

  /// clip calculated priority to reasonable range
  int clamp_recovery_priority(int prio, int pool_recovery_prio, int max);
  /// get log recovery reservation priority
  unsigned get_recovery_priority();
  /// get backfill reservation priority
  unsigned get_backfill_priority();
  /// get priority for pg deletion
  unsigned get_delete_priority();

  void try_mark_clean();  ///< mark an active pg clean

  /// return [start,end) bounds for required past_intervals
  static pair<epoch_t, epoch_t> get_required_past_interval_bounds(
    const pg_info_t &info,
    epoch_t oldest_map) {
    epoch_t start = std::max(
      info.history.last_epoch_clean ? info.history.last_epoch_clean :
       info.history.epoch_pool_created,
      oldest_map);
    epoch_t end = std::max(
      info.history.same_interval_since,
      info.history.epoch_pool_created);
    return make_pair(start, end);
  }
  void check_past_interval_bounds() const;
  PastIntervals::PriorSet build_prior();

  bool adjust_need_up_thru(const OSDMapRef osdmap);

  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  virtual void dump_recovery_info(Formatter *f) const = 0;

  void calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    ceph_assert(!acting_recovery_backfill.empty());
    for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      if (peer_last_complete_ondisk.count(*i) == 0)
	return;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[*i];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return;
    min_last_complete_ondisk = min;
    return;
  }

  virtual void calc_trim_to() = 0;

  virtual void calc_trim_to_aggressive() = 0;

  void proc_replica_log(pg_info_t &oinfo, const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from);
  void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, pg_shard_t from);
  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ObjectStore::Transaction *t;
    PGLogEntryHandler(PG *pg, ObjectStore::Transaction *t) : pg(pg), t(t) {}

    // LogEntryHandler
    void remove(const hobject_t &hoid) override {
      pg->get_pgbackend()->remove(hoid, t);
    }
    void try_stash(const hobject_t &hoid, version_t v) override {
      pg->get_pgbackend()->try_stash(hoid, v, t);
    }
    void rollback(const pg_log_entry_t &entry) override {
      ceph_assert(entry.can_rollback());
      pg->get_pgbackend()->rollback(entry, t);
    }
    void rollforward(const pg_log_entry_t &entry) override {
      pg->get_pgbackend()->rollforward(entry, t);
    }
    void trim(const pg_log_entry_t &entry) override {
      pg->get_pgbackend()->trim(entry, t);
    }
  };
  
  void update_object_snap_mapping(
    ObjectStore::Transaction *t, const hobject_t &soid,
    const set<snapid_t> &snaps);
  void clear_object_snap_mapping(
    ObjectStore::Transaction *t, const hobject_t &soid);
  void remove_snap_mapped_object(
    ObjectStore::Transaction& t, const hobject_t& soid);
  void merge_log(
    ObjectStore::Transaction& t, pg_info_t &oinfo,
    pg_log_t &olog, pg_shard_t from);
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead);
  bool search_for_missing(
    const pg_info_t &oinfo, const pg_missing_t &omissing,
    pg_shard_t fromosd,
    PeeringCtx*);

  void discover_all_missing(std::map<int, map<spg_t,pg_query_t> > &query_map);
  
  map<pg_shard_t, pg_info_t>::const_iterator find_best_info(
    const map<pg_shard_t, pg_info_t> &infos,
    bool restrict_to_up_acting,
    bool *history_les_bound) const;
  static void calc_ec_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    ostream &ss);
  static void calc_replicated_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    uint64_t force_auth_primary_missing_objects,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    pg_shard_t up_primary,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    const OSDMapRef osdmap,
    ostream &ss);
  void choose_async_recovery_ec(const map<pg_shard_t, pg_info_t> &all_info,
                                const pg_info_t &auth_info,
                                vector<int> *want,
                                set<pg_shard_t> *async_recovery,
                                const OSDMapRef osdmap) const;
  void choose_async_recovery_replicated(const map<pg_shard_t, pg_info_t> &all_info,
                                        const pg_info_t &auth_info,
                                        vector<int> *want,
                                        set<pg_shard_t> *async_recovery,
                                        const OSDMapRef osdmap) const;

  bool recoverable_and_ge_min_size(const vector<int> &want) const;
  bool choose_acting(pg_shard_t &auth_log_shard,
		     bool restrict_to_up_acting,
		     bool *history_les_bound);
  void build_might_have_unfound();
  void activate(
    ObjectStore::Transaction& t,
    epoch_t activation_epoch,
    map<int, map<spg_t,pg_query_t> >& query_map,
    map<int,
      vector<pair<pg_notify_t, PastIntervals> > > *activator_map,
    PeeringCtx *ctx);

  struct C_PG_ActivateCommitted : public Context {
    PGRef pg;
    epoch_t epoch;
    epoch_t activation_epoch;
    C_PG_ActivateCommitted(PG *p, epoch_t e, epoch_t ae)
      : pg(p), epoch(e), activation_epoch(ae) {}
    void finish(int r) override {
      pg->_activate_committed(epoch, activation_epoch);
    }
  };
  void _activate_committed(epoch_t epoch, epoch_t activation_epoch);
  void all_activated_and_committed();

  void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);

  bool have_unfound() const { 
    return missing_loc.have_unfound();
  }
  uint64_t get_num_unfound() const {
    return missing_loc.num_unfound();
  }
  bool all_missing_unfound() const {
    const auto& missing = pg_log.get_missing();
    if (!missing.have_missing())
      return false;
    for (auto& m : missing.get_items()) {
      if (!missing_loc.is_unfound(m.first))
        return false;
    }
    return true;
  }

  virtual void check_local() = 0;

  void purge_strays();

  void update_heartbeat_peers(set<int> peers) override;

  Context *finish_sync_event;

  Context *finish_recovery();
  void _finish_recovery(Context *c);
  struct C_PG_FinishRecovery : public Context {
    PGRef pg;
    explicit C_PG_FinishRecovery(PG *p) : pg(p) {}
    void finish(int r) override {
      pg->_finish_recovery(this);
    }
  };
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits) = 0;

  friend class C_OSD_RepModify_Commit;
  friend class C_DeleteMore;

  // -- backoff --
  Mutex backoff_lock;  // orders inside Backoff::lock
  map<hobject_t,set<BackoffRef>> backoffs;

  void add_backoff(SessionRef s, const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& o) {
    release_backoffs(o, o);
  }
  void clear_backoffs();

  void add_pg_backoff(SessionRef s) {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    add_backoff(s, begin, end);
  }
  void release_pg_backoffs() {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    release_backoffs(begin, end);
  }

  // -- scrub --
public:
  struct Scrubber {
    Scrubber();
    ~Scrubber();

    // metadata
    set<pg_shard_t> reserved_peers;
    bool reserved, reserve_failed;
    epoch_t epoch_start;

    // common to both scrubs
    bool active;
    set<pg_shard_t> waiting_on_whom;
    int shallow_errors;
    int deep_errors;
    int fixed;
    ScrubMap primary_scrubmap;
    ScrubMapBuilder primary_scrubmap_pos;
    epoch_t replica_scrub_start = 0;
    ScrubMap replica_scrubmap;
    ScrubMapBuilder replica_scrubmap_pos;
    map<pg_shard_t, ScrubMap> received_maps;
    OpRequestRef active_rep_scrub;
    utime_t scrub_reg_stamp;  // stamp we registered for

    omap_stat_t omap_stats  = (const struct omap_stat_t){ 0 };

    // For async sleep
    bool sleeping = false;
    bool needs_sleep = true;
    utime_t sleep_start;

    // flags to indicate explicitly requested scrubs (by admin)
    bool must_scrub, must_deep_scrub, must_repair;

    // Priority to use for scrub scheduling
    unsigned priority = 0;

    // this flag indicates whether we would like to do auto-repair of the PG or not
    bool auto_repair;
    // this flag indicates that we are scrubbing post repair to verify everything is fixed
    bool check_repair;
    // this flag indicates that if a regular scrub detects errors <= osd_scrub_auto_repair_num_errors,
    // we should deep scrub in order to auto repair
    bool deep_scrub_on_error;

    // Maps from objects with errors to missing/inconsistent peers
    map<hobject_t, set<pg_shard_t>> missing;
    map<hobject_t, set<pg_shard_t>> inconsistent;

    // Map from object with errors to good peers
    map<hobject_t, list<pair<ScrubMap::object, pg_shard_t> >> authoritative;

    // Cleaned map pending snap metadata scrub
    ScrubMap cleaned_meta_map;

    void clean_meta_map(ScrubMap &for_meta_scrub) {
      if (end.is_max() ||
          cleaned_meta_map.objects.empty()) {
         cleaned_meta_map.swap(for_meta_scrub);
      } else {
        auto iter = cleaned_meta_map.objects.end();
        --iter; // not empty, see if clause
        auto begin = cleaned_meta_map.objects.begin();
        if (iter->first.has_snapset()) {
          ++iter;
        } else {
          while (iter != begin) {
            auto next = iter--;
            if (next->first.get_head() != iter->first.get_head()) {
	      ++iter;
	      break;
            }
          }
        }
        for_meta_scrub.objects.insert(begin, iter);
        cleaned_meta_map.objects.erase(begin, iter);
      }
    }

    // digest updates which we are waiting on
    int num_digest_updates_pending;

    // chunky scrub
    hobject_t start, end;    // [start,end)
    hobject_t max_end;       // Largest end that may have been sent to replicas
    eversion_t subset_last_update;

    // chunky scrub state
    enum State {
      INACTIVE,
      NEW_CHUNK,
      WAIT_PUSHES,
      WAIT_LAST_UPDATE,
      BUILD_MAP,
      BUILD_MAP_DONE,
      WAIT_REPLICAS,
      COMPARE_MAPS,
      WAIT_DIGEST_UPDATES,
      FINISH,
      BUILD_MAP_REPLICA,
    } state;

    std::unique_ptr<Scrub::Store> store;
    // deep scrub
    bool deep;
    int preempt_left;
    int preempt_divisor;

    list<Context*> callbacks;
    void add_callback(Context *context) {
      callbacks.push_back(context);
    }
    void run_callbacks() {
      list<Context*> to_run;
      to_run.swap(callbacks);
      for (list<Context*>::iterator i = to_run.begin();
	   i != to_run.end();
	   ++i) {
	(*i)->complete(0);
      }
    }

    static const char *state_string(const PG::Scrubber::State& state) {
      const char *ret = NULL;
      switch( state )
      {
        case INACTIVE: ret = "INACTIVE"; break;
        case NEW_CHUNK: ret = "NEW_CHUNK"; break;
        case WAIT_PUSHES: ret = "WAIT_PUSHES"; break;
        case WAIT_LAST_UPDATE: ret = "WAIT_LAST_UPDATE"; break;
        case BUILD_MAP: ret = "BUILD_MAP"; break;
        case BUILD_MAP_DONE: ret = "BUILD_MAP_DONE"; break;
        case WAIT_REPLICAS: ret = "WAIT_REPLICAS"; break;
        case COMPARE_MAPS: ret = "COMPARE_MAPS"; break;
        case WAIT_DIGEST_UPDATES: ret = "WAIT_DIGEST_UPDATES"; break;
        case FINISH: ret = "FINISH"; break;
        case BUILD_MAP_REPLICA: ret = "BUILD_MAP_REPLICA"; break;
      }
      return ret;
    }

    bool is_chunky_scrub_active() const { return state != INACTIVE; }

    // clear all state
    void reset() {
      active = false;
      waiting_on_whom.clear();
      if (active_rep_scrub) {
        active_rep_scrub = OpRequestRef();
      }
      received_maps.clear();

      must_scrub = false;
      must_deep_scrub = false;
      must_repair = false;
      auto_repair = false;
      check_repair = false;
      deep_scrub_on_error = false;

      state = PG::Scrubber::INACTIVE;
      start = hobject_t();
      end = hobject_t();
      max_end = hobject_t();
      subset_last_update = eversion_t();
      shallow_errors = 0;
      deep_errors = 0;
      fixed = 0;
      omap_stats = (const struct omap_stat_t){ 0 };
      deep = false;
      run_callbacks();
      inconsistent.clear();
      missing.clear();
      authoritative.clear();
      num_digest_updates_pending = 0;
      primary_scrubmap = ScrubMap();
      primary_scrubmap_pos.reset();
      replica_scrubmap = ScrubMap();
      replica_scrubmap_pos.reset();
      cleaned_meta_map = ScrubMap();
      sleeping = false;
      needs_sleep = true;
      sleep_start = utime_t();
    }

    void create_results(const hobject_t& obj);
    void cleanup_store(ObjectStore::Transaction *t);
  } scrubber;

protected:
  bool scrub_after_recovery;

  int active_pushes;

  bool scrub_can_preempt = false;
  bool scrub_preempted = false;

  // we allow some number of preemptions of the scrub, which mean we do
  // not block.  then we start to block.  once we start blocking, we do
  // not stop until the scrub range is completed.
  bool write_blocked_by_scrub(const hobject_t &soid);

  /// true if the given range intersects the scrub interval in any way
  bool range_intersects_scrub(const hobject_t &start, const hobject_t& end);

  void repair_object(
    const hobject_t& soid, list<pair<ScrubMap::object, pg_shard_t> > *ok_peers,
    pg_shard_t bad_peer);

  void chunky_scrub(ThreadPool::TPHandle &handle);
  void scrub_compare_maps();
  /**
   * return true if any inconsistency/missing is repaired, false otherwise
   */
  bool scrub_process_inconsistent();
  bool ops_blocked_by_scrub() const;
  void scrub_finish();
  void scrub_clear_state(bool keep_repair = false);
  void _scan_snaps(ScrubMap &map);
  void _repair_oinfo_oid(ScrubMap &map);
  void _scan_rollback_obs(const vector<ghobject_t> &rollback_obs);
  void _request_scrub_map(pg_shard_t replica, eversion_t version,
                          hobject_t start, hobject_t end, bool deep,
			  bool allow_preemption);
  int build_scrub_map_chunk(
    ScrubMap &map,
    ScrubMapBuilder &pos,
    hobject_t start, hobject_t end, bool deep,
    ThreadPool::TPHandle &handle);
  /**
   * returns true if [begin, end) is good to scrub at this time
   * a false return value obliges the implementer to requeue scrub when the
   * condition preventing scrub clears
   */
  virtual bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end) = 0;
  virtual void scrub_snapshot_metadata(
    ScrubMap &map,
    const std::map<hobject_t,
                   pair<boost::optional<uint32_t>,
                        boost::optional<uint32_t>>> &missing_digest) { }
  virtual void _scrub_clear_state() { }
  virtual void _scrub_finish() { }
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;

  void replica_scrub(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_replica_scrub_map(OpRequestRef op);

  void handle_scrub_reserve_request(OpRequestRef op);
  void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from);
  void handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from);
  void handle_scrub_reserve_release(OpRequestRef op);

  void reject_reservation();
  void schedule_backfill_retry(float retry);
  void schedule_recovery_retry(float retry);

  // -- recovery state --

  template <class EVT>
  struct QueuePeeringEvt : Context {
    PGRef pg;
    epoch_t epoch;
    EVT evt;
    QueuePeeringEvt(PG *pg, epoch_t epoch, EVT evt) :
      pg(pg), epoch(epoch), evt(evt) {}
    void finish(int r) override {
      pg->lock();
      pg->queue_peering_event(PGPeeringEventRef(
				new PGPeeringEvent(
				  epoch,
				  epoch,
				  evt)));
      pg->unlock();
    }
  };


public:
  int pg_stat_adjust(osd_stat_t *new_stat);
protected:
  bool delete_needs_sleep = false;

protected:
  void reset_min_peer_features() {
    peer_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  }
  uint64_t get_min_peer_features() const { return peer_features; }
  void apply_peer_features(uint64_t f) { peer_features &= f; }

  uint64_t get_min_acting_features() const { return acting_features; }
  uint64_t get_min_upacting_features() const { return upacting_features; }
  bool perform_deletes_during_peering() const {
    return !(get_osdmap()->test_flag(CEPH_OSDMAP_RECOVERY_DELETES));
  }

  bool hard_limit_pglog() const {
    return (get_osdmap()->test_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT));
  }

  void init_primary_up_acting(
    const vector<int> &newup,
    const vector<int> &newacting,
    int new_up_primary,
    int new_acting_primary) {
    actingset.clear();
    acting = newacting;
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] != CRUSH_ITEM_NONE)
	actingset.insert(
	  pg_shard_t(
	    acting[i],
	    pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    upset.clear();
    up = newup;
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] != CRUSH_ITEM_NONE)
	upset.insert(
	  pg_shard_t(
	    up[i],
	    pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    if (!pool.info.is_erasure()) {
      up_primary = pg_shard_t(new_up_primary, shard_id_t::NO_SHARD);
      primary = pg_shard_t(new_acting_primary, shard_id_t::NO_SHARD);
      return;
    }
    up_primary = pg_shard_t();
    primary = pg_shard_t();
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] == new_up_primary) {
	up_primary = pg_shard_t(up[i], shard_id_t(i));
	break;
      }
    }
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] == new_acting_primary) {
	primary = pg_shard_t(acting[i], shard_id_t(i));
	break;
      }
    }
    ceph_assert(up_primary.osd == new_up_primary);
    ceph_assert(primary.osd == new_acting_primary);
  }

  void set_role(int r) {
    role = r;
  }

  bool state_test(uint64_t m) const { return recovery_state.state_test(m); }
  void state_set(uint64_t m) { recovery_state.state_set(m); }
  void state_clear(uint64_t m) { recovery_state.state_clear(m); }

  bool is_complete() const {
    return recovery_state.is_complete();
  }
  bool should_send_notify() const {
    return recovery_state.should_send_notify();
  }

  int get_state() const { return state; }
  bool is_active() const { return recovery_state.is_active(); }
  bool is_activating() const { return recovery_state.is_activating(); }
  bool is_peering() const { return recovery_state.is_peering(); }
  bool is_down() const { return recovery_state.is_down(); }
  bool is_recovery_unfound() const { return state_test(PG_STATE_RECOVERY_UNFOUND); }
  bool is_backfill_unfound() const { return state_test(PG_STATE_BACKFILL_UNFOUND); }
  bool is_incomplete() const { return recovery_state.is_incomplete(); }
  bool is_clean() const { return recovery_state.is_clean(); }
  bool is_degraded() const { return recovery_state.is_degraded(); }
  bool is_undersized() const { return recovery_state.is_undersized(); }
  bool is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }
  bool is_remapped() const { return recovery_state.is_remapped(); }
  bool is_peered() const { return recovery_state.is_peered(); }
  bool is_recovering() const { return state_test(PG_STATE_RECOVERING); }
  bool is_premerge() const { return recovery_state.is_premerge(); }
  bool is_repair() const { return recovery_state.is_repair(); }

  bool is_empty() const { return recovery_state.is_empty(); }

  // pg on-disk state
  void do_pending_flush();

public:
  static void _create(ObjectStore::Transaction& t, spg_t pgid, int bits);
  static void _init(ObjectStore::Transaction& t,
		    spg_t pgid, const pg_pool_t *pool);

  virtual void prepare_write(
    pg_info_t &info,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ObjectStore::Transaction &t) override;

  void prepare_write_info(
    bool dirty_info,
    bool dirty_big_info,
    bool need_update_epoch,
    map<string,bufferlist> *km);

  void update_store_with_options(const pool_opts_t &opts) override;

  static int _prepare_write_info(
    CephContext* cct,
    map<string,bufferlist> *km,
    epoch_t epoch,
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    bool dirty_big_info,
    bool dirty_epoch,
    bool try_fast_info,
    PerfCounters *logger = nullptr);

  void write_if_dirty(PeeringCtx *rctx) {
    write_if_dirty(*rctx->transaction);
  }
protected:
  void write_if_dirty(ObjectStore::Transaction& t) {
    recovery_state.write_if_dirty(t);
  }

  PGLog::IndexedLog projected_log;
  bool check_in_progress_op(
    const osd_reqid_t &r,
    eversion_t *version,
    version_t *user_version,
    int *return_code) const;
  eversion_t projected_last_update;
  eversion_t get_next_version() const {
    eversion_t at_version(
      get_osdmap_epoch(),
      projected_last_update.version+1);
    ceph_assert(at_version > info.last_update);
    ceph_assert(at_version > pg_log.get_head());
    ceph_assert(at_version > projected_last_update);
    return at_version;
  }

  void add_log_entry(const pg_log_entry_t& e, bool applied);
  void append_log(
    const vector<pg_log_entry_t>& logv,
    eversion_t trim_to,
    eversion_t roll_forward_to,
    ObjectStore::Transaction &t,
    bool transaction_applied = true,
    bool async = false);
  bool check_log_for_corruption(ObjectStore *store);

  std::string get_corrupt_pg_log_name() const;

  void update_snap_map(
    const vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(vector<snapid_t> &snaps);

  void log_weirdness();

  virtual void kick_snap_trim() = 0;
  virtual void snap_trimmer_scrub_complete() = 0;
  bool requeue_scrub(bool high_priority = false);
  void queue_recovery();
  bool queue_scrub();
  unsigned get_scrub_priority();

  /// share pg info after a pg is active
  void share_pg_info();


  bool append_log_entries_update_missing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    boost::optional<eversion_t> trim_to,
    boost::optional<eversion_t> roll_forward_to);

  /**
   * Merge entries updating missing as necessary on all
   * acting_recovery_backfill logs and missings (also missing_loc)
   */
  void merge_new_log_entries(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    boost::optional<eversion_t> trim_to,
    boost::optional<eversion_t> roll_forward_to);

  bool try_flush_or_schedule_async() override;
  void start_peering_interval(
    const OSDMapRef lastmap,
    const vector<int>& newup, int up_primary,
    const vector<int>& newacting, int acting_primary,
    ObjectStore::Transaction *t);
  void on_new_interval();
  virtual void _on_new_interval() = 0;
  void start_flush_on_transaction(
    ObjectStore::Transaction *t) override;

  void update_history(const pg_history_t& history) {
    recovery_state.update_history(history);
  }
  void fulfill_info(pg_shard_t from, const pg_query_t &query,
		    pair<pg_shard_t, pg_info_t> &notify_info);
  void fulfill_log(pg_shard_t from, const pg_query_t &query, epoch_t query_epoch);
  void fulfill_query(const MQuery& q, PeeringCtx *rctx);
  void check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap);

  bool should_restart_peering(
    int newupprimary,
    int newactingprimary,
    const vector<int>& newup,
    const vector<int>& newacting,
    OSDMapRef lastmap,
    OSDMapRef osdmap);

  // OpRequest queueing
  bool can_discard_op(OpRequestRef& op);
  bool can_discard_scan(OpRequestRef op);
  bool can_discard_backfill(OpRequestRef op);
  bool can_discard_request(OpRequestRef& op);

  template<typename T, int MSGTYPE>
  bool can_discard_replica_op(OpRequestRef& op);

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);
  bool old_peering_evt(PGPeeringEventRef evt) {
    return old_peering_msg(evt->get_epoch_sent(), evt->get_epoch_requested());
  }
  static bool have_same_or_newer_map(epoch_t cur_epoch, epoch_t e) {
    return e <= cur_epoch;
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef& op);


  // recovery bits
  void take_waiters();


  // abstract bits
  friend class FlushState;

  friend ostream& operator<<(ostream& out, const PG& pg);
};


ostream& operator<<(ostream& out, const PG::BackfillInterval& bi);

#endif
