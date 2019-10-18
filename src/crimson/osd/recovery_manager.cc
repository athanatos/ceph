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

#include "crimson/osd/recovery_manager.h"

namespace crimson::osd {

// clear state.  called on recovery completion AND cancellation.
void RecoveryManager::_clear_recovery_state()
{
#if 0
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  last_backfill_started = hobject_t();
  set<hobject_t>::iterator i = backfills_in_flight.begin();
  while (i != backfills_in_flight.end()) {
    ceph_assert(recovering.count(*i));
    backfills_in_flight.erase(i++);
  }

  list<OpRequestRef> blocked_ops;
  for (map<hobject_t, ObjectContextRef>::iterator i = recovering.begin();
       i != recovering.end();
       recovering.erase(i++)) {
    if (i->second) {
      i->second->drop_recovery_read(&blocked_ops);
      requeue_ops(blocked_ops);
    }
  }
  ceph_assert(backfills_in_flight.empty());
  pending_backfill_updates.clear();
  ceph_assert(recovering.empty());
  pgbackend->clear_recovery_state();
#endif
}

void RecoveryManager::cancel_pull(const hobject_t &soid)
{
#if 0
  dout(20) << __func__ << ": " << soid << dendl;
  ceph_assert(recovering.count(soid));
  ObjectContextRef obc = recovering[soid];
  if (obc) {
    list<OpRequestRef> blocked_ops;
    obc->drop_recovery_read(&blocked_ops);
    requeue_ops(blocked_ops);
  }
  recovering.erase(soid);
  finish_recovery_op(soid);
  release_backoffs(soid);
  if (waiting_for_degraded_object.count(soid)) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  if (waiting_for_unreadable_object.count(soid)) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(waiting_for_unreadable_object[soid]);
    waiting_for_unreadable_object.erase(soid);
  }
  if (is_missing_object(soid))
    recovery_state.set_last_requested(0);
  finish_degraded_object(soid);
#endif
}

void RecoveryManager::check_recovery_sources(const OSDMapRef& osdmap)
{
#if 0
  pgbackend->check_recovery_sources(osdmap);
#endif
}

bool RecoveryManager::start_recovery_ops(
  uint64_t max,
  ThreadPool::TPHandle &handle,
  uint64_t *ops_started)
{
#if 0
  uint64_t& started = *ops_started;
  started = 0;
  bool work_in_progress = false;
  bool recovery_started = false;
  ceph_assert(is_primary());
  ceph_assert(is_peered());
  ceph_assert(!recovery_state.is_deleting());

  ceph_assert(recovery_queued);
  recovery_queued = false;

  if (!state_test(PG_STATE_RECOVERING) &&
      !state_test(PG_STATE_BACKFILLING)) {
    /* TODO: I think this case is broken and will make do_recovery()
     * unhappy since we're returning false */
    dout(10) << "recovery raced and were queued twice, ignoring!" << dendl;
    return have_unfound();
  }

  const auto &missing = recovery_state.get_pg_log().get_missing();

  uint64_t num_unfound = get_num_unfound();

  if (!recovery_state.have_missing()) {
    recovery_state.local_recovery_complete();
  }

  if (!missing.have_missing() || // Primary does not have missing
      // or all of the missing objects are unfound.
      recovery_state.all_missing_unfound()) {
    // Recover the replicas.
    started = recover_replicas(max, handle, &recovery_started);
  }
  if (!started) {
    // We still have missing objects that we should grab from replicas.
    started += recover_primary(max, handle);
  }
  if (!started && num_unfound != get_num_unfound()) {
    // second chance to recovery replicas
    started = recover_replicas(max, handle, &recovery_started);
  }

  if (started || recovery_started)
    work_in_progress = true;

  bool deferred_backfill = false;
  if (recovering.empty() &&
      state_test(PG_STATE_BACKFILLING) &&
      !get_backfill_targets().empty() && started < max &&
      missing.num_missing() == 0 &&
      waiting_on_backfill.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
      dout(10) << "deferring backfill due to NOBACKFILL" << dendl;
      deferred_backfill = true;
    } else if (get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) &&
	       !is_degraded())  {
      dout(10) << "deferring backfill due to NOREBALANCE" << dendl;
      deferred_backfill = true;
    } else if (!recovery_state.is_backfill_reserved()) {
      dout(10) << "deferring backfill due to !backfill_reserved" << dendl;
      if (!backfill_reserving) {
	dout(10) << "queueing RequestBackfill" << dendl;
	backfill_reserving = true;
	queue_peering_event(
	  PGPeeringEventRef(
	    std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::RequestBackfill())));
      }
      deferred_backfill = true;
    } else {
      started += recover_backfill(max - started, handle, &work_in_progress);
    }
  }

  dout(10) << " started " << started << dendl;
  osd->logger->inc(l_osd_rop, started);

  if (!recovering.empty() ||
      work_in_progress || recovery_ops_active > 0 || deferred_backfill)
    return !work_in_progress && have_unfound();

  ceph_assert(recovering.empty());
  ceph_assert(recovery_ops_active == 0);

  dout(10) << __func__ << " needs_recovery: "
	   << recovery_state.get_missing_loc().get_needs_recovery()
	   << dendl;
  dout(10) << __func__ << " missing_loc: "
	   << recovery_state.get_missing_loc().get_missing_locs()
	   << dendl;
  int unfound = get_num_unfound();
  if (unfound) {
    dout(10) << " still have " << unfound << " unfound" << dendl;
    return true;
  }

  if (missing.num_missing() > 0) {
    // this shouldn't happen!
    osd->clog->error() << info.pgid << " Unexpected Error: recovery ending with "
		       << missing.num_missing() << ": " << missing.get_items();
    return false;
  }

  if (needs_recovery()) {
    // this shouldn't happen!
    // We already checked num_missing() so we must have missing replicas
    osd->clog->error() << info.pgid
                       << " Unexpected Error: recovery ending with missing replicas";
    return false;
  }

  if (state_test(PG_STATE_RECOVERING)) {
    state_clear(PG_STATE_RECOVERING);
    state_clear(PG_STATE_FORCED_RECOVERY);
    if (needs_backfill()) {
      dout(10) << "recovery done, queuing backfill" << dendl;
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::RequestBackfill())));
    } else {
      dout(10) << "recovery done, no backfill" << dendl;
      eio_errors_to_process = false;
      state_clear(PG_STATE_FORCED_BACKFILL);
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::AllReplicasRecovered())));
    }
  } else { // backfilling
    state_clear(PG_STATE_BACKFILLING);
    state_clear(PG_STATE_FORCED_BACKFILL);
    state_clear(PG_STATE_FORCED_RECOVERY);
    dout(10) << "recovery done, backfill done" << dendl;
    eio_errors_to_process = false;
    queue_peering_event(
      PGPeeringEventRef(
        std::make_shared<PGPeeringEvent>(
          get_osdmap_epoch(),
          get_osdmap_epoch(),
          PeeringState::Backfilled())));
  }

  return false;
#endif
  return true;
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
uint64_t RecoveryManager::recover_primary(
  uint64_t max, ThreadPool::TPHandle &handle)
{
#if 0
  ceph_assert(is_primary());

  const auto &missing = recovery_state.get_pg_log().get_missing();

  dout(10) << __func__ << " recovering " << recovering.size()
           << " in pg,"
           << " missing " << missing << dendl;

  dout(25) << __func__ << " " << missing.get_items() << dendl;

  // look at log!
  pg_log_entry_t *latest = 0;
  unsigned started = 0;
  int skipped = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  map<version_t, hobject_t>::const_iterator p =
    missing.get_rmissing().lower_bound(recovery_state.get_pg_log().get_log().last_requested);
  while (p != missing.get_rmissing().end()) {
    handle.reset_tp_timeout();
    hobject_t soid;
    version_t v = p->first;

    auto it_objects = recovery_state.get_pg_log().get_log().objects.find(p->second);
    if (it_objects != recovery_state.get_pg_log().get_log().objects.end()) {
      latest = it_objects->second;
      ceph_assert(latest->is_update() || latest->is_delete());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    const pg_missing_item& item = missing.get_items().find(p->second)->second;
    ++p;

    hobject_t head = soid.get_head();

    eversion_t need = item.need;

    dout(10) << __func__ << " "
             << soid << " " << item.need
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (recovering.count(soid) ? " (recovering)":"")
	     << (recovering.count(head) ? " (recovering head)":"")
             << dendl;

    if (latest) {
      switch (latest->op) {
      case pg_log_entry_t::CLONE:
	/*
	 * Handling for this special case removed for now, until we
	 * can correctly construct an accurate SnapSet from the old
	 * one.
	 */
	break;

      case pg_log_entry_t::LOST_REVERT:
	{
	  if (item.have == latest->reverting_to) {
	    ObjectContextRef obc = get_object_context(soid, true);

	    if (obc->obs.oi.version == latest->version) {
	      // I'm already reverting
	      dout(10) << " already reverting " << soid << dendl;
	    } else {
	      dout(10) << " reverting " << soid << " to " << latest->prior_version << dendl;
	      obc->obs.oi.version = latest->version;

	      ObjectStore::Transaction t;
	      bufferlist b2;
	      obc->obs.oi.encode(
		b2,
		get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
	      ceph_assert(!pool.info.require_rollback());
	      t.setattr(coll, ghobject_t(soid), OI_ATTR, b2);

	      recovery_state.recover_got(
		soid,
		latest->version,
		false,
		t);

	      ++active_pushes;

	      t.register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));
	      t.register_on_commit(new C_OSD_CommittedPushedObject(
				     this,
				     get_osdmap_epoch(),
				     info.last_complete));
	      osd->store->queue_transaction(ch, std::move(t));
	      continue;
	    }
	  } else {
	    /*
	     * Pull the old version of the object.  Update missing_loc here to have the location
	     * of the version we want.
	     *
	     * This doesn't use the usual missing_loc paths, but that's okay:
	     *  - if we have it locally, we hit the case above, and go from there.
	     *  - if we don't, we always pass through this case during recovery and set up the location
	     *    properly.
	     *  - this way we don't need to mangle the missing code to be general about needing an old
	     *    version...
	     */
	    eversion_t alternate_need = latest->reverting_to;
	    dout(10) << " need to pull prior_version " << alternate_need << " for revert " << item << dendl;

	    set<pg_shard_t> good_peers;
	    for (auto p = recovery_state.get_peer_missing().begin();
		 p != recovery_state.get_peer_missing().end();
		 ++p) {
	      if (p->second.is_missing(soid, need) &&
		  p->second.get_items().at(soid).have == alternate_need) {
		good_peers.insert(p->first);
	      }
	    }
	    recovery_state.set_revert_with_targets(
	      soid,
	      good_peers);
	    dout(10) << " will pull " << alternate_need << " or " << need
		     << " from one of "
		     << recovery_state.get_missing_loc().get_locations(soid)
		     << dendl;
	  }
	}
	break;
      }
    }

    if (!recovering.count(soid)) {
      if (recovering.count(head)) {
	++skipped;
      } else {
	int r = recover_missing(
	  soid, need, get_recovery_op_priority(), h);
	switch (r) {
	case PULL_YES:
	  ++started;
	  break;
	case PULL_HEAD:
	  ++started;
	case PULL_NONE:
	  ++skipped;
	  break;
	default:
	  ceph_abort();
	}
	if (started >= max)
	  break;
      }
    }

    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      recovery_state.set_last_requested(v);
  }

  pgbackend->run_recovery_op(h, get_recovery_op_priority());
  return started;
#endif
  return 0;
}

bool RecoveryManager::primary_error(
  const hobject_t& soid, eversion_t v)
{
#if 0
  recovery_state.force_object_missing(pg_whoami, soid, v);
  bool uhoh = recovery_state.get_missing_loc().is_unfound(soid);
  if (uhoh)
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid << ", unfound";
  else
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid
		       << ", will try copies on "
		       << recovery_state.get_missing_loc().get_locations(soid);
  return uhoh;
#endif
  return true;
}

int RecoveryManager::prep_object_replica_deletes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
#if 0
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  ObjectContextRef obc = get_object_context(soid, false);
  if (obc) {
    if (!obc->get_recovery_read()) {
      dout(20) << "replica delete delayed on " << soid
	       << "; could not get rw_manager lock" << dendl;
      *work_started = true;
      return 0;
    } else {
      dout(20) << "replica delete got recovery read lock on " << soid
	       << dendl;
    }
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  if (!obc)
    recovering.insert(make_pair(soid, ObjectContextRef()));
  else
    recovering.insert(make_pair(soid, obc));

  pgbackend->recover_delete_object(soid, v, h);
  return 1;
#endif
  return 0;
}

int RecoveryManager::prep_object_replica_pushes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
#if 0
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid.get_head();
    if (recovery_state.get_pg_log().get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return 0;
      } else {
	int r = recover_missing(
	    head, recovery_state.get_pg_log().get_missing().get_items().find(head)->second.need,
	    get_recovery_op_priority(), h);
	if (r != PULL_NONE)
	  return 1;
	return 0;
      }
    }
  }

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc) {
    primary_error(soid, v);
    return 0;
  }

  if (!obc->get_recovery_read()) {
    dout(20) << "recovery delayed on " << soid
	     << "; could not get rw_manager lock" << dendl;
    *work_started = true;
    return 0;
  } else {
    dout(20) << "recovery got recovery read lock on " << soid
	     << dendl;
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));

  int r = pgbackend->recover_object(
    soid,
    v,
    ObjectContextRef(),
    obc, // has snapset context
    h);
  if (r < 0) {
    dout(0) << __func__ << " Error " << r << " on oid " << soid << dendl;
    on_failed_pull({ pg_whoami }, soid, v);
    return 0;
  }
  return 1;
#endif
  return 0;
}

uint64_t RecoveryManager::recover_replicas(
  uint64_t max, ThreadPool::TPHandle &handle,
  bool *work_started)
{
#if 0
  dout(10) << __func__ << "(" << max << ")" << dendl;
  uint64_t started = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();

  // this is FAR from an optimal recovery order.  pretty lame, really.
  ceph_assert(!get_acting_recovery_backfill().empty());
  // choose replicas to recover, replica has the shortest missing list first
  // so we can bring it back to normal ASAP
  std::vector<std::pair<unsigned int, pg_shard_t>> replicas_by_num_missing,
    async_by_num_missing;
  replicas_by_num_missing.reserve(get_acting_recovery_backfill().size() - 1);
  for (auto &p: get_acting_recovery_backfill()) {
    if (p == get_primary()) {
      continue;
    }
    auto pm = recovery_state.get_peer_missing().find(p);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    auto nm = pm->second.num_missing();
    if (nm != 0) {
      if (is_async_recovery_target(p)) {
        async_by_num_missing.push_back(make_pair(nm, p));
      } else {
        replicas_by_num_missing.push_back(make_pair(nm, p));
      }
    }
  }
  // sort by number of missing objects, in ascending order.
  auto func = [](const std::pair<unsigned int, pg_shard_t> &lhs,
                 const std::pair<unsigned int, pg_shard_t> &rhs) {
    return lhs.first < rhs.first;
  };
  // acting goes first
  std::sort(replicas_by_num_missing.begin(), replicas_by_num_missing.end(), func);
  // then async_recovery_targets
  std::sort(async_by_num_missing.begin(), async_by_num_missing.end(), func);
  replicas_by_num_missing.insert(replicas_by_num_missing.end(),
    async_by_num_missing.begin(), async_by_num_missing.end());
  for (auto &replica: replicas_by_num_missing) {
    pg_shard_t &peer = replica.second;
    ceph_assert(peer != get_primary());
    auto pm = recovery_state.get_peer_missing().find(peer);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    dout(20) << " peer osd." << peer << " missing " << pm->second.get_items() << dendl;

    // oldest first!
    const pg_missing_t &m(pm->second);
    for (map<version_t, hobject_t>::const_iterator p = m.get_rmissing().begin();
	 p != m.get_rmissing().end() && started < max;
	   ++p) {
      handle.reset_tp_timeout();
      const hobject_t soid(p->second);

      if (recovery_state.get_missing_loc().is_unfound(soid)) {
	dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	continue;
      }

      const pg_info_t &pi = recovery_state.get_peer_info(peer);
      if (soid > pi.last_backfill) {
	if (!recovering.count(soid)) {
          derr << __func__ << ": object " << soid << " last_backfill "
	       << pi.last_backfill << dendl;
	  derr << __func__ << ": object added to missing set for backfill, but "
	       << "is not in recovering, error!" << dendl;
	  ceph_abort();
	}
	continue;
      }

      if (recovering.count(soid)) {
	dout(10) << __func__ << ": already recovering " << soid << dendl;
	continue;
      }

      if (recovery_state.get_missing_loc().is_deleted(soid)) {
	dout(10) << __func__ << ": " << soid << " is a delete, removing" << dendl;
	map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
	started += prep_object_replica_deletes(soid, r->second.need, h, work_started);
	continue;
      }

      if (soid.is_snap() &&
	  recovery_state.get_pg_log().get_missing().is_missing(
	    soid.get_head())) {
	dout(10) << __func__ << ": " << soid.get_head()
		 << " still missing on primary" << dendl;
	continue;
      }

      if (recovery_state.get_pg_log().get_missing().is_missing(soid)) {
	dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
      started += prep_object_replica_pushes(soid, r->second.need, h, work_started);
    }
  }

  pgbackend->run_recovery_op(h, get_recovery_op_priority());
  return started;
#endif
  return 0;
}

hobject_t RecoveryManager::earliest_peer_backfill() const
{
#if 0
  hobject_t e = hobject_t::get_max();
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t peer = *i;
    map<pg_shard_t, BackfillInterval>::const_iterator iter =
      peer_backfill_info.find(peer);
    ceph_assert(iter != peer_backfill_info.end());
    if (iter->second.begin < e)
      e = iter->second.begin;
  }
  return e;
#endif
  return hobject_t();
}

bool RecoveryManager::all_peer_done() const
{
#if 0
  // Primary hasn't got any more objects
  ceph_assert(backfill_info.empty());

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    map<pg_shard_t, BackfillInterval>::const_iterator piter =
      peer_backfill_info.find(bt);
    ceph_assert(piter != peer_backfill_info.end());
    const BackfillInterval& pbi = piter->second;
    // See if peer has more to process
    if (!pbi.extends_to_end() || !pbi.empty())
	return false;
  }
  return true;
#endif
  return true;
}

/**
 * recover_backfill
 *
 * Invariants:
 *
 * backfilled: fully pushed to replica or present in replica's missing set (both
 * our copy and theirs).
 *
 * All objects on a backfill_target in
 * [MIN,peer_backfill_info[backfill_target].begin) are valid; logically-removed
 * objects have been actually deleted and all logically-valid objects are replicated.
 * There may be PG objects in this interval yet to be backfilled.
 *
 * All objects in PG in [MIN,backfill_info.begin) have been backfilled to all
 * backfill_targets.  There may be objects on backfill_target(s) yet to be deleted.
 *
 * For a backfill target, all objects < std::min(peer_backfill_info[target].begin,
 *     backfill_info.begin) in PG are backfilled.  No deleted objects in this
 * interval remain on the backfill target.
 *
 * For a backfill target, all objects <= peer_info[target].last_backfill
 * have been backfilled to target
 *
 * There *MAY* be missing/outdated objects between last_backfill_started and
 * std::min(peer_backfill_info[*].begin, backfill_info.begin) in the event that client
 * io created objects since the last scan.  For this reason, we call
 * update_range() again before continuing backfill.
 */
uint64_t RecoveryManager::recover_backfill(
  uint64_t max,
  ThreadPool::TPHandle &handle, bool *work_started)
{
#if 0
  dout(10) << __func__ << " (" << max << ")"
           << " bft=" << get_backfill_targets()
	   << " last_backfill_started " << last_backfill_started
	   << (new_backfill ? " new_backfill":"")
	   << dendl;
  ceph_assert(!get_backfill_targets().empty());

  // Initialize from prior backfill state
  if (new_backfill) {
    // on_activate() was called prior to getting here
    ceph_assert(last_backfill_started == earliest_backfill());
    new_backfill = false;

    // initialize BackfillIntervals
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      peer_backfill_info[*i].reset(
	recovery_state.get_peer_info(*i).last_backfill);
    }
    backfill_info.reset(last_backfill_started);

    backfills_in_flight.clear();
    pending_backfill_updates.clear();
  }

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    dout(10) << "peer osd." << *i
	   << " info " << recovery_state.get_peer_info(*i)
	   << " interval " << peer_backfill_info[*i].begin
	   << "-" << peer_backfill_info[*i].end
	   << " " << peer_backfill_info[*i].objects.size() << " objects"
	   << dendl;
  }

  // update our local interval to cope with recent changes
  backfill_info.begin = last_backfill_started;
  update_range(&backfill_info, handle);

  unsigned ops = 0;
  vector<boost::tuple<hobject_t, eversion_t, pg_shard_t> > to_remove;
  set<hobject_t> add_to_stat;

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    peer_backfill_info[*i].trim_to(
      std::max(
	recovery_state.get_peer_info(*i).last_backfill,
	last_backfill_started));
  }
  backfill_info.trim_to(last_backfill_started);

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  while (ops < max) {
    if (backfill_info.begin <= earliest_peer_backfill() &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      hobject_t next = backfill_info.end;
      backfill_info.reset(next);
      backfill_info.end = hobject_t::get_max();
      update_range(&backfill_info, handle);
      backfill_info.trim();
    }

    dout(20) << "   my backfill interval " << backfill_info << dendl;

    bool sent_scan = false;
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      pg_shard_t bt = *i;
      BackfillInterval& pbi = peer_backfill_info[bt];

      dout(20) << " peer shard " << bt << " backfill " << pbi << dendl;
      if (pbi.begin <= backfill_info.begin &&
	  !pbi.extends_to_end() && pbi.empty()) {
	dout(10) << " scanning peer osd." << bt << " from " << pbi.end << dendl;
	epoch_t e = get_osdmap_epoch();
	MOSDPGScan *m = new MOSDPGScan(
	  MOSDPGScan::OP_SCAN_GET_DIGEST, pg_whoami, e, get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard),
	  pbi.end, hobject_t());
	osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
	ceph_assert(waiting_on_backfill.find(bt) == waiting_on_backfill.end());
	waiting_on_backfill.insert(bt);
        sent_scan = true;
      }
    }

    // Count simultaneous scans as a single op and let those complete
    if (sent_scan) {
      ops++;
      start_recovery_op(hobject_t::get_max()); // XXX: was pbi.end
      break;
    }

    if (backfill_info.empty() && all_peer_done()) {
      dout(10) << " reached end for both local and all peers" << dendl;
      break;
    }

    // Get object within set of peers to operate on and
    // the set of targets for which that object applies.
    hobject_t check = earliest_peer_backfill();

    if (check < backfill_info.begin) {

      set<pg_shard_t> check_targets;
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        if (pbi.begin == check)
          check_targets.insert(bt);
      }
      ceph_assert(!check_targets.empty());

      dout(20) << " BACKFILL removing " << check
	       << " from peers " << check_targets << dendl;
      for (set<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        ceph_assert(pbi.begin == check);

        to_remove.push_back(boost::make_tuple(check, pbi.objects.begin()->second, bt));
        pbi.pop_front();
      }

      last_backfill_started = check;

      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else {
      eversion_t& obj_v = backfill_info.objects.begin()->second;

      vector<pg_shard_t> need_ver_targs, missing_targs, keep_ver_targs, skip_targs;
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
	pg_shard_t bt = *i;
	BackfillInterval& pbi = peer_backfill_info[bt];
        // Find all check peers that have the wrong version
	if (check == backfill_info.begin && check == pbi.begin) {
	  if (pbi.objects.begin()->second != obj_v) {
	    need_ver_targs.push_back(bt);
	  } else {
	    keep_ver_targs.push_back(bt);
	  }
        } else {
	  const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

          // Only include peers that we've caught up to their backfill line
	  // otherwise, they only appear to be missing this object
	  // because their pbi.begin > backfill_info.begin.
          if (backfill_info.begin > pinfo.last_backfill)
	    missing_targs.push_back(bt);
	  else
	    skip_targs.push_back(bt);
	}
      }

      if (!keep_ver_targs.empty()) {
        // These peers have version obj_v
	dout(20) << " BACKFILL keeping " << check
		 << " with ver " << obj_v
		 << " on peers " << keep_ver_targs << dendl;
	//assert(!waiting_for_degraded_object.count(check));
      }
      if (!need_ver_targs.empty() || !missing_targs.empty()) {
	ObjectContextRef obc = get_object_context(backfill_info.begin, false);
	ceph_assert(obc);
	if (obc->get_recovery_read()) {
	  if (!need_ver_targs.empty()) {
	    dout(20) << " BACKFILL replacing " << check
		   << " with ver " << obj_v
		   << " to peers " << need_ver_targs << dendl;
	  }
	  if (!missing_targs.empty()) {
	    dout(20) << " BACKFILL pushing " << backfill_info.begin
	         << " with ver " << obj_v
	         << " to peers " << missing_targs << dendl;
	  }
	  vector<pg_shard_t> all_push = need_ver_targs;
	  all_push.insert(all_push.end(), missing_targs.begin(), missing_targs.end());

	  handle.reset_tp_timeout();
	  int r = prep_backfill_object_push(backfill_info.begin, obj_v, obc, all_push, h);
	  if (r < 0) {
	    *work_started = true;
	    dout(0) << __func__ << " Error " << r << " trying to backfill " << backfill_info.begin << dendl;
	    break;
	  }
	  ops++;
	} else {
	  *work_started = true;
	  dout(20) << "backfill blocking on " << backfill_info.begin
		   << "; could not get rw_manager lock" << dendl;
	  break;
	}
      }
      dout(20) << "need_ver_targs=" << need_ver_targs
	       << " keep_ver_targs=" << keep_ver_targs << dendl;
      dout(20) << "backfill_targets=" << get_backfill_targets()
	       << " missing_targs=" << missing_targs
	       << " skip_targs=" << skip_targs << dendl;

      last_backfill_started = backfill_info.begin;
      add_to_stat.insert(backfill_info.begin); // XXX: Only one for all pushes?
      backfill_info.pop_front();
      vector<pg_shard_t> check_targets = need_ver_targs;
      check_targets.insert(check_targets.end(), keep_ver_targs.begin(), keep_ver_targs.end());
      for (vector<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        pbi.pop_front();
      }
    }
  }

  hobject_t backfill_pos =
    std::min(backfill_info.begin, earliest_peer_backfill());

  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContextRef obc = get_object_context(*i, false);
    ceph_assert(obc);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
  }
  map<pg_shard_t,MOSDPGBackfillRemove*> reqs;
  for (unsigned i = 0; i < to_remove.size(); ++i) {
    handle.reset_tp_timeout();
    const hobject_t& oid = to_remove[i].get<0>();
    eversion_t v = to_remove[i].get<1>();
    pg_shard_t peer = to_remove[i].get<2>();
    MOSDPGBackfillRemove *m;
    auto it = reqs.find(peer);
    if (it != reqs.end()) {
      m = it->second;
    } else {
      m = reqs[peer] = new MOSDPGBackfillRemove(
	spg_t(info.pgid.pgid, peer.shard),
	get_osdmap_epoch());
    }
    m->ls.push_back(make_pair(oid, v));

    if (oid <= last_backfill_started)
      pending_backfill_updates[oid]; // add empty stat!
  }
  for (auto p : reqs) {
    osd->send_message_osd_cluster(p.first.osd, p.second,
				  get_osdmap_epoch());
  }

  pgbackend->run_recovery_op(h, get_recovery_op_priority());

  dout(5) << "backfill_pos is " << backfill_pos << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  hobject_t next_backfill_to_complete = backfills_in_flight.empty() ?
    backfill_pos : *(backfills_in_flight.begin());
  hobject_t new_last_backfill = earliest_backfill();
  dout(10) << "starting new_last_backfill at " << new_last_backfill << dendl;
  for (map<hobject_t, pg_stat_t>::iterator i =
	 pending_backfill_updates.begin();
       i != pending_backfill_updates.end() &&
	 i->first < next_backfill_to_complete;
       pending_backfill_updates.erase(i++)) {
    dout(20) << " pending_backfill_update " << i->first << dendl;
    ceph_assert(i->first > new_last_backfill);
    recovery_state.update_complete_backfill_object_stats(
      i->first,
      i->second);
    new_last_backfill = i->first;
  }
  dout(10) << "possible new_last_backfill at " << new_last_backfill << dendl;

  ceph_assert(!pending_backfill_updates.empty() ||
	 new_last_backfill == last_backfill_started);
  if (pending_backfill_updates.empty() &&
      backfill_pos.is_max()) {
    ceph_assert(backfills_in_flight.empty());
    new_last_backfill = backfill_pos;
    last_backfill_started = backfill_pos;
  }
  dout(10) << "final new_last_backfill at " << new_last_backfill << dendl;

  // If new_last_backfill == MAX, then we will send OP_BACKFILL_FINISH to
  // all the backfill targets.  Otherwise, we will move last_backfill up on
  // those targets need it and send OP_BACKFILL_PROGRESS to them.
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

    if (new_last_backfill > pinfo.last_backfill) {
      recovery_state.update_peer_last_backfill(bt, new_last_backfill);
      epoch_t e = get_osdmap_epoch();
      MOSDPGBackfill *m = NULL;
      if (pinfo.last_backfill.is_max()) {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_FINISH,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
        start_recovery_op(hobject_t::get_max());
      } else {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
      }
      m->last_backfill = pinfo.last_backfill;
      m->stats = pinfo.stats;
      osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
      dout(10) << " peer " << bt
	       << " num_objects now " << pinfo.stats.stats.sum.num_objects
	       << " / " << info.stats.stats.sum.num_objects << dendl;
    }
  }

  if (ops)
    *work_started = true;
  return ops;
#endif
  return 0;
}

int RecoveryManager::prep_backfill_object_push(
  hobject_t oid, eversion_t v,
  ObjectContextRef obc,
  vector<pg_shard_t> peers,
  PGBackend::RecoveryHandle *h)
{
#if 0
  dout(10) << __func__ << " " << oid << " v " << v << " to peers " << peers << dendl;
  ceph_assert(!peers.empty());

  backfills_in_flight.insert(oid);
  recovery_state.prepare_backfill_for_missing(oid, v, peers);

  ceph_assert(!recovering.count(oid));

  start_recovery_op(oid);
  recovering.insert(make_pair(oid, obc));

  int r = pgbackend->recover_object(
    oid,
    v,
    ObjectContextRef(),
    obc,
    h);
  if (r < 0) {
    dout(0) << __func__ << " Error " << r << " on oid " << oid << dendl;
    on_failed_pull({ pg_whoami }, oid, v);
  }
  return r;
#endif
  return 0;
}

void RecoveryManager::update_range(
  BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
#if 0
  int local_min = cct->_conf->osd_backfill_scan_min;
  int local_max = cct->_conf->osd_backfill_scan_max;

  if (bi->version < info.log_tail) {
    dout(10) << __func__<< ": bi is old, rescanning local backfill_info"
	     << dendl;
    bi->version = info.last_update;
    scan_range(local_min, local_max, bi, handle);
  }

  if (bi->version >= projected_last_update) {
    dout(10) << __func__<< ": bi is current " << dendl;
    ceph_assert(bi->version == projected_last_update);
  } else if (bi->version >= info.log_tail) {
    if (recovery_state.get_pg_log().get_log().empty() && projected_log.empty()) {
      /* Because we don't move log_tail on split, the log might be
       * empty even if log_tail != last_update.  However, the only
       * way to get here with an empty log is if log_tail is actually
       * eversion_t(), because otherwise the entry which changed
       * last_update since the last scan would have to be present.
       */
      ceph_assert(bi->version == eversion_t());
      return;
    }

    dout(10) << __func__<< ": bi is old, (" << bi->version
	     << ") can be updated with log to projected_last_update "
	     << projected_last_update << dendl;

    auto func = [&](const pg_log_entry_t &e) {
      dout(10) << __func__ << ": updating from version " << e.version
               << dendl;
      const hobject_t &soid = e.soid;
      if (soid >= bi->begin &&
	  soid < bi->end) {
	if (e.is_update()) {
	  dout(10) << __func__ << ": " << e.soid << " updated to version "
		   << e.version << dendl;
	  bi->objects.erase(e.soid);
	  bi->objects.insert(
	    make_pair(
	      e.soid,
	      e.version));
	} else if (e.is_delete()) {
	  dout(10) << __func__ << ": " << e.soid << " removed" << dendl;
	  bi->objects.erase(e.soid);
	}
      }
    };
    dout(10) << "scanning pg log first" << dendl;
    recovery_state.get_pg_log().get_log().scan_log_after(bi->version, func);
    dout(10) << "scanning projected log" << dendl;
    projected_log.scan_log_after(bi->version, func);
    bi->version = projected_last_update;
  } else {
    ceph_abort_msg("scan_range should have raised bi->version past log_tail");
  }
#endif
}

void RecoveryManager::scan_range(
  int min, int max, BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
#if 0
  ceph_assert(is_locked());
  dout(10) << "scan_range from " << bi->begin << dendl;
  bi->clear_objects();

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = pgbackend->objects_list_partial(bi->begin, min, max, &ls, &bi->end);
  ceph_assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    handle.reset_tp_timeout();
    ObjectContextRef obc;
    if (is_primary())
      obc = object_contexts.lookup(*p);
    if (obc) {
      if (!obc->obs.exists) {
	/* If the object does not exist here, it must have been removed
	 * between the collection_list_partial and here.  This can happen
	 * for the first item in the range, which is usually last_backfill.
	 */
	continue;
      }
      bi->objects[*p] = obc->obs.oi.version;
      dout(20) << "  " << *p << " " << obc->obs.oi.version << dendl;
    } else {
      bufferlist bl;
      int r = pgbackend->objects_get_attr(*p, OI_ATTR, &bl);
      /* If the object does not exist here, it must have been removed
       * between the collection_list_partial and here.  This can happen
       * for the first item in the range, which is usually last_backfill.
       */
      if (r == -ENOENT)
	continue;

      ceph_assert(r >= 0);
      object_info_t oi(bl);
      bi->objects[*p] = oi.version;
      dout(20) << "  " << *p << " " << oi.version << dendl;
    }
  }
#endif
}

}
