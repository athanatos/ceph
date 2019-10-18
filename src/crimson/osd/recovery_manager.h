// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/WorkQueue.h"
#include "common/hobject.h"
#include "osd/osd_types.h"
#include "osd/OSDMap.h"
#include "crimson/osd/pg_backend.h"

namespace crimson::osd {

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

/*
 * RecoveryManager
 *
 * Manages ongoing recovery and backfill status
 *
 * peer_info[backfill_target].last_backfill == info.last_backfill on the peer.
 *
 * objects prior to peer_info[backfill_target].last_backfill
 *   - are on the peer
 *   - are included in the peer stats
 *
 * objects \in (last_backfill, last_backfill_started]
 *   - are on the peer or are in backfills_in_flight
 *   - are not included in pg stats (yet)
 *   - have their stats in pending_backfill_updates on the primary
 */
class RecoveryManager {
  set<hobject_t> backfills_in_flight;
  map<hobject_t, pg_stat_t> pending_backfill_updates;


  /// last backfill operation started
  hobject_t last_backfill_started;
  bool new_backfill;

  void _clear_recovery_state();
  void cancel_pull(const hobject_t &soid);
  void check_recovery_sources(const OSDMapRef& osdmap);

  uint64_t recover_primary(uint64_t max, ThreadPool::TPHandle &handle);

  bool primary_error(
    const hobject_t& soid, eversion_t v);

  int prep_object_replica_deletes(
    const hobject_t& soid, eversion_t v,
    PGBackend::RecoveryHandle *h,
    bool *work_started);

  int prep_object_replica_pushes(
    const hobject_t& soid, eversion_t v,
    PGBackend::RecoveryHandle *h,
    bool *work_started);

  uint64_t recover_replicas(
    uint64_t max, ThreadPool::TPHandle &handle,
    bool *work_started);

  hobject_t earliest_peer_backfill() const;

  bool all_peer_done() const;

  uint64_t recover_backfill(
    uint64_t max,
    ThreadPool::TPHandle &handle, bool *work_started);

  int prep_backfill_object_push(
    hobject_t oid, eversion_t v,
    ObjectContextRef obc,
    vector<pg_shard_t> peers,
    PGBackend::RecoveryHandle *h);

  void update_range(
    BackfillInterval *bi,
    ThreadPool::TPHandle &handle);

  void scan_range(
    int min, int max, BackfillInterval *bi,
    ThreadPool::TPHandle &handle);

  bool start_recovery_ops(
    uint64_t max,
    ThreadPool::TPHandle &handle,
    uint64_t *ops_started);

public:
  void dump_recovery_state(Formatter *f) const {
    // TODO
  }
};

}
