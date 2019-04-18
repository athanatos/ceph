// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGPeeringEvent.h"
#include "common/dout.h"
#include "PeeringState.h"
#include "PG.h"
#include "OSD.h"

#include "messages/MOSDPGRemove.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDScrubReserve.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd

void PGPool::update(CephContext *cct, OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  if (!pi) {
    return; // pool has been deleted
  }
  info = *pi;
  name = map->get_pool_name(id);

  bool updated = false;
  if ((map->get_epoch() != cached_epoch + 1) ||
      (pi->get_snap_epoch() == map->get_epoch())) {
    updated = true;
  }

  if (map->require_osd_release >= CEPH_RELEASE_MIMIC) {
    // mimic tracks removed_snaps_queue in the OSDmap and purged_snaps
    // in the pg_info_t, with deltas for both in each OSDMap.  we don't
    // need to (and can't) track it here.
    cached_removed_snaps.clear();
    newly_removed_snaps.clear();
  } else {
    // legacy (<= luminous) removed_snaps tracking
    if (updated) {
      if (pi->maybe_updated_removed_snaps(cached_removed_snaps)) {
	pi->build_removed_snaps(newly_removed_snaps);
	if (cached_removed_snaps.subset_of(newly_removed_snaps)) {
          interval_set<snapid_t> removed_snaps = newly_removed_snaps;
          newly_removed_snaps.subtract(cached_removed_snaps);
          cached_removed_snaps.swap(removed_snaps);
	} else {
          lgeneric_subdout(cct, osd, 0) << __func__
		<< " cached_removed_snaps shrank from " << cached_removed_snaps
		<< " to " << newly_removed_snaps << dendl;
          cached_removed_snaps.swap(newly_removed_snaps);
          newly_removed_snaps.clear();
	}
      } else {
	newly_removed_snaps.clear();
      }
    } else {
      /* 1) map->get_epoch() == cached_epoch + 1 &&
       * 2) pi->get_snap_epoch() != map->get_epoch()
       *
       * From the if branch, 1 && 2 must be true.  From 2, we know that
       * this map didn't change the set of removed snaps.  From 1, we
       * know that our cached_removed_snaps matches the previous map.
       * Thus, from 1 && 2, cached_removed snaps matches the current
       * set of removed snaps and all we have to do is clear
       * newly_removed_snaps.
       */
      newly_removed_snaps.clear();
    }
    lgeneric_subdout(cct, osd, 20)
      << "PGPool::update cached_removed_snaps "
      << cached_removed_snaps
      << " newly_removed_snaps "
      << newly_removed_snaps
      << " snapc " << snapc
      << (updated ? " (updated)":" (no change)")
      << dendl;
    if (cct->_conf->osd_debug_verify_cached_snaps) {
      interval_set<snapid_t> actual_removed_snaps;
      pi->build_removed_snaps(actual_removed_snaps);
      if (!(actual_removed_snaps == cached_removed_snaps)) {
	lgeneric_derr(cct) << __func__
		   << ": mismatch between the actual removed snaps "
		   << actual_removed_snaps
		   << " and pool.cached_removed_snaps "
		   << " pool.cached_removed_snaps " << cached_removed_snaps
		   << dendl;
      }
      ceph_assert(actual_removed_snaps == cached_removed_snaps);
    }
  }
  if (info.is_pool_snaps_mode() && updated) {
    snapc = pi->get_snap_context();
  }
  cached_epoch = map->get_epoch();
}

void PeeringState::PeeringMachine::send_query(
  pg_shard_t to, const pg_query_t &query) {
  ceph_assert(state->rctx);
  ceph_assert(state->rctx->query_map);
  (*state->rctx->query_map)[to.osd][
    spg_t(context< PeeringMachine >().spgid.pgid, to.shard)] = query;
}

/*-------------Peering State Helpers----------------*/
#undef dout_prefix
#define dout_prefix (dpp->gen_prefix(*_dout))
#undef psdout
#define psdout(x) ldout(cct, x)

PeeringState::PeeringState(
  CephContext *cct,
  spg_t spgid,
  const PGPool &_pool,
  OSDMapRef curmap,
  DoutPrefixProvider *dpp,
  PeeringListener *pl,
  PG *pg)
  : state_history(pg),
    machine(this, cct, spgid, dpp, pl, pg, &state_history), cct(cct),
    spgid(spgid),
    dpp(dpp),
    pl(pl),
    pg(pg),
    orig_ctx(0),
    osdmap_ref(curmap),
    pool(_pool),
    info(spgid),
    pg_log(cct),
    missing_loc(spgid, this, dpp, cct)
{
  machine.initiate();
}

void PeeringState::check_recovery_sources(const OSDMapRef& osdmap)
{
  /*
   * check that any peers we are planning to (or currently) pulling
   * objects from are dealt with.
   */
  missing_loc.check_recovery_sources(osdmap);
  pl->check_recovery_sources(osdmap);

  for (set<pg_shard_t>::iterator i = peer_log_requested.begin();
       i != peer_log_requested.end();
       ) {
    if (!osdmap->is_up(i->osd)) {
      dout(10) << "peer_log_requested removing " << *i << dendl;
      peer_log_requested.erase(i++);
    } else {
      ++i;
    }
  }

  for (set<pg_shard_t>::iterator i = peer_missing_requested.begin();
       i != peer_missing_requested.end();
       ) {
    if (!osdmap->is_up(i->osd)) {
      dout(10) << "peer_missing_requested removing " << *i << dendl;
      peer_missing_requested.erase(i++);
    } else {
      ++i;
    }
  }
}

void PeeringState::update_history(const pg_history_t& new_history)
{
  if (info.history.merge(new_history)) {
    psdout(20) << __func__ << " advanced history from " << new_history << dendl;
    dirty_info = true;
    if (info.history.last_epoch_clean >= info.history.same_interval_since) {
      psdout(20) << __func__ << " clearing past_intervals" << dendl;
      past_intervals.clear();
      dirty_big_info = true;
    }
  }
  pl->on_info_history_change();
}

void PeeringState::purge_strays()
{
  if (is_premerge()) {
    psdout(10) << "purge_strays " << stray_set << " but premerge, doing nothing"
	       << dendl;
    return;
  }
  if (cct->_conf.get_val<bool>("osd_debug_no_purge_strays")) {
    return;
  }
  psdout(10) << "purge_strays " << stray_set << dendl;

  bool removed = false;
  for (set<pg_shard_t>::iterator p = stray_set.begin();
       p != stray_set.end();
       ++p) {
    ceph_assert(!is_acting_recovery_backfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap_epoch(),
	to_remove);
      pl->send_cluster_message(p->osd, m, get_osdmap_epoch());
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_missing.erase(*p);
    peer_info.erase(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}


bool PeeringState::proc_replica_info(
  pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch)
{
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  if (!get_osdmap()->has_been_up_since(from.osd, send_epoch)) {
    dout(10) << " got info " << oinfo << " from down osd." << from
	     << " discarding" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  ceph_assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);

  update_history(oinfo.history);

  // stray?
  if (!is_up(from) && !is_acting(from)) {
    dout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end())
    update_heartbeat_peers();

  return true;
}


void PeeringState::remove_down_peer_info(const OSDMapRef &osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first.osd)) {
      psdout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_purged.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  check_recovery_sources(osdmap);
}

void PeeringState::update_heartbeat_peers()
{
  if (!is_primary())
    return;

  set<int> new_peers;
  for (unsigned i=0; i<acting.size(); i++) {
    if (acting[i] != CRUSH_ITEM_NONE)
      new_peers.insert(acting[i]);
  }
  for (unsigned i=0; i<up.size(); i++) {
    if (up[i] != CRUSH_ITEM_NONE)
      new_peers.insert(up[i]);
  }
  for (map<pg_shard_t,pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       ++p) {
    new_peers.insert(p->first.osd);
  }
  pl->update_heartbeat_peers(std::move(new_peers));
}

void PeeringState::write_if_dirty(ObjectStore::Transaction& t)
{
  pl->prepare_write(
    info,
    pg_log,
    dirty_info,
    dirty_big_info,
    last_persisted_osdmap < get_osdmap_epoch(),
    t);
  last_persisted_osdmap = get_osdmap_epoch();
}

void PeeringState::advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  PeeringCtx *rctx)
{
  ceph_assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  ceph_assert(lastmap == osdmap_ref);
  psdout(10) << "handle_advance_map "
	    << newup << "/" << newacting
	    << " -- " << up_primary << "/" << acting_primary
	    << dendl;

  update_osdmap_ref(osdmap);
  pool.update(cct, osdmap);

  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);
  handle_event(evt, rctx);
  if (pool.info.last_change == osdmap_ref->get_epoch()) {
    pl->on_pool_change();
  }
  last_require_osd_release = osdmap->require_osd_release;
}

void PeeringState::activate_map(PeeringCtx *rctx)
{
  psdout(10) << __func__ << dendl;
  ActMap evt;
  handle_event(evt, rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    psdout(20) << __func__ << ": Dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    psdout(20) << __func__ << ": Not dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  write_if_dirty(*rctx->transaction);

  if (get_osdmap()->check_new_blacklist_entries()) {
    pl->check_blacklisted_watchers();
  }
}

void PeeringState::set_last_peering_reset()
{
  psdout(20) << "set_last_peering_reset " << get_osdmap_epoch() << dendl;
  if (last_peering_reset != get_osdmap_epoch()) {
    dout(10) << "Clearing blocked outgoing recovery messages" << dendl;
    clear_blocked_outgoing();
    if (!pl->try_flush_or_schedule_async()) {
      psdout(10) << "Beginning to block outgoing recovery messages" << dendl;
      begin_block_outgoing();
    } else {
      psdout(10) << "Not blocking outgoing recovery messages" << dendl;
    }
  }
}

void PeeringState::complete_flush()
{
  flushes_in_progress--;
  if (flushes_in_progress == 0) {
    pl->on_flushed();
  }
}

void PeeringState::check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap)
{
  bool changed = false;
  if (osdmap->test_flag(CEPH_OSDMAP_FULL) &&
      !lastmap->test_flag(CEPH_OSDMAP_FULL)) {
    psdout(10) << " cluster was marked full in "
	       << osdmap->get_epoch() << dendl;
    changed = true;
  }
  const pg_pool_t *pi = osdmap->get_pg_pool(info.pgid.pool());
  if (!pi) {
    return; // pool deleted
  }
  if (pi->has_flag(pg_pool_t::FLAG_FULL)) {
    const pg_pool_t *opi = lastmap->get_pg_pool(info.pgid.pool());
    if (!opi || !opi->has_flag(pg_pool_t::FLAG_FULL)) {
      psdout(10) << " pool was marked full in " << osdmap->get_epoch() << dendl;
      changed = true;
    }
  }
  if (changed) {
    info.history.last_epoch_marked_full = osdmap->get_epoch();
    dirty_info = true;
  }
}

bool PeeringState::should_restart_peering(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup,
  const vector<int>& newacting,
  OSDMapRef lastmap,
  OSDMapRef osdmap)
{
  if (PastIntervals::is_new_interval(
	primary.osd,
	newactingprimary,
	acting,
	newacting,
	up_primary.osd,
	newupprimary,
	up,
	newup,
	osdmap,
	lastmap,
	info.pgid.pgid)) {
    dout(20) << "new interval newup " << newup
	     << " newacting " << newacting << dendl;
    return true;
  }
  if (!lastmap->is_up(pg_whoami.osd) && osdmap->is_up(pg_whoami.osd)) {
    psdout(10) << __func__ << " osd transitioned from down -> up"
	       << dendl;
    return true;
  }
  return false;
}

/* Called before initializing peering during advance_map */
void PeeringState::start_peering_interval(
  const OSDMapRef lastmap,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  ObjectStore::Transaction *t)
{
  const OSDMapRef osdmap = get_osdmap();

  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();

  if (is_primary()) {
    pl->clear_ready_to_merge();
  }


  pg_shard_t old_acting_primary = get_primary();
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary();
  bool was_old_replica = is_replica();

  acting.swap(oldacting);
  up.swap(oldup);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  if (info.stats.up != up ||
      info.stats.acting != acting ||
      info.stats.up_primary != new_up_primary ||
      info.stats.acting_primary != new_acting_primary) {
    info.stats.up = up;
    info.stats.up_primary = new_up_primary;
    info.stats.acting = acting;
    info.stats.acting_primary = new_acting_primary;
    info.stats.mapping_epoch = osdmap->get_epoch();
  }

  pl->clear_publish_stats();

  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  int role = osdmap->calc_pg_role(pg_whoami.osd, acting, acting.size());
  if (pool.info.is_replicated() || role == pg_whoami.shard)
    set_role(role);
  else
    set_role(-1);

  // did acting, up, primary|acker change?
  if (!lastmap) {
    psdout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
    info.history.same_interval_since = osdmap->get_epoch();
  } else {
    std::stringstream debug;
    ceph_assert(info.history.same_interval_since != 0);
    bool new_interval = PastIntervals::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      old_up_primary.osd,
      new_up_primary,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap,
      info.pgid.pgid,
      missing_loc.get_recoverable_predicate(),
      &past_intervals,
      &debug);
    psdout(10) << __func__ << ": check_new_interval output: "
	       << debug.str() << dendl;
    if (new_interval) {
      if (osdmap->get_epoch() == pl->oldest_stored_osdmap() &&
	  info.history.last_epoch_clean < osdmap->get_epoch()) {
	psdout(10) << " map gap, clearing past_intervals and faking" << dendl;
	// our information is incomplete and useless; someone else was clean
	// after everything we know if osdmaps were trimmed.
	past_intervals.clear();
      } else {
	psdout(10) << " noting past " << past_intervals << dendl;
      }
      dirty_info = true;
      dirty_big_info = true;
      info.history.same_interval_since = osdmap->get_epoch();
      if (osdmap->have_pg_pool(info.pgid.pgid.pool()) &&
	  info.pgid.pgid.is_split(lastmap->get_pg_num(info.pgid.pgid.pool()),
				  osdmap->get_pg_num(info.pgid.pgid.pool()),
				  nullptr)) {
	info.history.last_epoch_split = osdmap->get_epoch();
      }
    }
  }

  if (old_up_primary != up_primary ||
      oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  pl->on_new_interval();
  pl->on_info_history_change();

  psdout(1) << __func__ << " up " << oldup << " -> " << up
	    << ", acting " << oldacting << " -> " << acting
	    << ", acting_primary " << old_acting_primary << " -> "
	    << new_acting_primary
	    << ", up_primary " << old_up_primary << " -> " << new_up_primary
	    << ", role " << oldrole << " -> " << role
	    << ", features acting " << acting_features
	    << " upacting " << upacting_features
	    << dendl;

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_PEERED);
  state_clear(PG_STATE_PREMERGE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERY_TOOFULL);
  state_clear(PG_STATE_RECOVERING);

  peer_purged.clear();
  acting_recovery_backfill.clear();

  // reset primary/replica state?
  if (was_old_primary || is_primary()) {
    pl->clear_want_pg_temp();
  } else if (was_old_replica || is_replica()) {
    pl->clear_want_pg_temp();
  }
  clear_primary_state();

  pl->on_change(t);

  ceph_assert(!deleting);

  // should we tell the primary we are here?
  send_notify = !is_primary();

  if (role != oldrole ||
      was_old_primary != is_primary()) {
    // did primary change?
    if (was_old_primary != is_primary()) {
      state_clear(PG_STATE_CLEAN);
    }

    pl->on_role_change();
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != old_acting_primary) {
      psdout(10) << oldacting << " -> " << acting
	       << ", acting primary "
	       << old_acting_primary << " -> " << get_primary()
	       << dendl;
    } else {
      // primary is the same.
      if (is_primary()) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);

	psdout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }

  if (acting.empty() && !up.empty() && up_primary == pg_whoami) {
    psdout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    pl->queue_want_pg_temp(acting);
  }
}

void PeeringState::on_new_interval()
{
  const OSDMapRef osdmap = get_osdmap();

  // initialize features
  acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    uint64_t f = osdmap->get_xinfo(*p).features;
    acting_features &= f;
    upacting_features &= f;
  }
  for (vector<int>::iterator p = up.begin(); p != up.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    upacting_features &= osdmap->get_xinfo(*p).features;
  }

  pl->on_new_interval();
}

void PeeringState::init_primary_up_acting(
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

void PeeringState::clear_primary_state()
{
  psdout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_bytes.clear();
  peer_missing.clear();
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  might_have_unfound.clear();
  need_up_thru = false;
  missing_loc.clear();
  pg_log.reset_recovery_pointers();
  pl->clear_primary_state();
}

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


void PeeringState::check_past_interval_bounds() const
{
  auto rpib = get_required_past_interval_bounds(
    info,
    pl->oldest_stored_osdmap());
  if (rpib.first >= rpib.second) {
    if (!past_intervals.empty()) {
      pl->get_clog().error() << info.pgid << " required past_interval bounds are"
			     << " empty [" << rpib << ") but past_intervals is not: "
			     << past_intervals;
      derr << info.pgid << " required past_interval bounds are"
	   << " empty [" << rpib << ") but past_intervals is not: "
	   << past_intervals << dendl;
    }
  } else {
    if (past_intervals.empty()) {
      pl->get_clog().error() << info.pgid << " required past_interval bounds are"
			     << " not empty [" << rpib << ") but past_intervals "
			     << past_intervals << " is empty";
      derr << info.pgid << " required past_interval bounds are"
	   << " not empty [" << rpib << ") but past_intervals "
	   << past_intervals << " is empty" << dendl;
      ceph_assert(!past_intervals.empty());
    }

    auto apib = past_intervals.get_bounds();
    if (apib.first > rpib.first) {
      pl->get_clog().error() << info.pgid << " past_intervals [" << apib
			     << ") start interval does not contain the required"
			     << " bound [" << rpib << ") start";
      derr << info.pgid << " past_intervals [" << apib
	   << ") start interval does not contain the required"
	   << " bound [" << rpib << ") start" << dendl;
      ceph_abort_msg("past_interval start interval mismatch");
    }
    if (apib.second != rpib.second) {
      pl->get_clog().error() << info.pgid << " past_interal bound [" << apib
			     << ") end does not match required [" << rpib
			     << ") end";
      derr << info.pgid << " past_interal bound [" << apib
	   << ") end does not match required [" << rpib
	   << ") end" << dendl;
      ceph_abort_msg("past_interval end mismatch");
    }
  }
}

inline int PeeringState::clamp_recovery_priority(int priority)
{
  static_assert(OSD_RECOVERY_PRIORITY_MIN < OSD_RECOVERY_PRIORITY_MAX, "Invalid priority range");
  static_assert(OSD_RECOVERY_PRIORITY_MIN >= 0, "Priority range must match unsigned type");

  // Clamp to valid range
  if (priority > OSD_RECOVERY_PRIORITY_MAX) {
    return OSD_RECOVERY_PRIORITY_MAX;
  } else if (priority < OSD_RECOVERY_PRIORITY_MIN) {
    return OSD_RECOVERY_PRIORITY_MIN;
  } else {
    return priority;
  }
}

unsigned PeeringState::get_recovery_priority()
{
  // a higher value -> a higher priority
  int64_t ret = 0;

  if (state & PG_STATE_FORCED_RECOVERY) {
    ret = OSD_RECOVERY_PRIORITY_FORCED;
  } else {
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &ret);
    ret = clamp_recovery_priority(OSD_RECOVERY_PRIORITY_BASE + ret);
  }
  psdout(20) << __func__ << " recovery priority " << " is " << ret
	     << ", state is " << state << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_backfill_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_BACKFILL_PRIORITY_BASE;
  if (state & PG_STATE_FORCED_BACKFILL) {
    ret = OSD_BACKFILL_PRIORITY_FORCED;
  } else {
    if (acting.size() < pool.info.min_size) {
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = OSD_BACKFILL_INACTIVE_PRIORITY_BASE + (pool.info.min_size - acting.size());

    } else if (is_undersized()) {
      // undersized: OSD_BACKFILL_DEGRADED_PRIORITY_BASE + num missing replicas
      ceph_assert(pool.info.size > actingset.size());
      ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE + (pool.info.size - actingset.size());

    } else if (is_degraded()) {
      // degraded: baseline degraded
      ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
    }

    // Adjust with pool's recovery priority
    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(pool_recovery_priority + ret);
  }

  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_delete_priority()
{
  auto state = get_osdmap()->get_state(pg_whoami.osd);
  if (state & (CEPH_OSD_BACKFILLFULL |
               CEPH_OSD_FULL)) {
    return OSD_DELETE_PRIORITY_FULL;
  } else if (state & CEPH_OSD_NEARFULL) {
    return OSD_DELETE_PRIORITY_FULLISH;
  } else {
    return OSD_DELETE_PRIORITY_NORMAL;
  }
}


bool PeeringState::set_force_recovery(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_RECOVERY) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_RECOVERING))) {
      psdout(20) << __func__ << " set" << dendl;
      state_set(PG_STATE_FORCED_RECOVERY);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_RECOVERY) {
    psdout(20) << __func__ << " clear" << dendl;
    state_clear(PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << __func__ << " state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_recovery_priority());
  }
  return did;
}

bool PeeringState::set_force_backfill(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_BACKFILL) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILLING))) {
      psdout(10) << __func__ << " set" << dendl;
      state_set(PG_STATE_FORCED_BACKFILL);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_BACKFILL) {
    psdout(10) << __func__ << " clear" << dendl;
    state_clear(PG_STATE_FORCED_BACKFILL);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << __func__ << " state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_backfill_priority());
  }
  return did;
}

bool PeeringState::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = osdmap->get_up_thru(pg_whoami.osd);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    psdout(10) << "adjust_need_up_thru now "
	       << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

PastIntervals::PriorSet PeeringState::build_prior()
{
  if (1) {
    // sanity check
    for (map<pg_shard_t,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 ++it) {
      ceph_assert(info.history.last_epoch_started >=
		  it->second.history.last_epoch_started);
    }
  }

  const OSDMap &osdmap = *get_osdmap();
  PastIntervals::PriorSet prior = past_intervals.get_prior_set(
    pool.info.is_erasure(),
    info.history.last_epoch_started,
    &missing_loc.get_recoverable_predicate(),
    [&](epoch_t start, int osd, epoch_t *lost_at) {
      const osd_info_t *pinfo = 0;
      if (osdmap.exists(osd)) {
	pinfo = &osdmap.get_info(osd);
	if (lost_at)
	  *lost_at = pinfo->lost_at;
      }

      if (osdmap.is_up(osd)) {
	return PastIntervals::UP;
      } else if (!pinfo) {
	return PastIntervals::DNE;
      } else if (pinfo->lost_at > start) {
	return PastIntervals::LOST;
      } else {
	return PastIntervals::DOWN;
      }
    },
    up,
    acting,
    dpp);

  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(pg_whoami.osd) <
      info.history.same_interval_since) {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " < same_since " << info.history.same_interval_since
	       << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " >= same_since " << info.history.same_interval_since
	       << ", all is well" << dendl;
    need_up_thru = false;
  }
  pl->set_probe_targets(prior.probe);
  return prior;
}

bool PeeringState::needs_recovery() const
{
  ceph_assert(is_primary());

  auto &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    dout(10) << __func__ << " primary has " << missing.num_missing()
      << " missing" << dendl;
    return true;
  }

  ceph_assert(!acting_recovery_backfill.empty());
  set<pg_shard_t>::const_iterator end = acting_recovery_backfill.end();
  set<pg_shard_t>::const_iterator a = acting_recovery_backfill.begin();
  for (; a != end; ++a) {
    if (*a == get_primary()) continue;
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      psdout(10) << __func__ << " osd." << peer << " doesn't have missing set"
		 << dendl;
      continue;
    }
    if (pm->second.num_missing()) {
      psdout(10) << __func__ << " osd." << peer << " has "
		 << pm->second.num_missing() << " missing" << dendl;
      return true;
    }
  }

  psdout(10) << __func__ << " is recovered" << dendl;
  return false;
}

bool PeeringState::needs_backfill() const
{
  ceph_assert(is_primary());

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  set<pg_shard_t>::const_iterator end = backfill_targets.end();
  set<pg_shard_t>::const_iterator a = backfill_targets.begin();
  for (; a != end; ++a) {
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (!pi->second.last_backfill.is_max()) {
      psdout(10) << __func__ << " osd." << peer
		 << " has last_backfill " << pi->second.last_backfill << dendl;
      return true;
    }
  }

  dout(10) << __func__ << " does not need backfill" << dendl;
  return false;
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PeeringState::all_unfound_are_queried_or_lost(
  const OSDMapRef osdmap) const
{
  ceph_assert(is_primary());

  set<pg_shard_t>::const_iterator peer = might_have_unfound.begin();
  set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    if (!osdmap->exists(peer->osd))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(peer->osd));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  psdout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound "
	     << might_have_unfound
	     << " have been queried or are marked lost" << dendl;
  return true;
}


void PeeringState::reject_reservation()
{
  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap_epoch()),
    get_osdmap_epoch());
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PeeringState::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos,
  bool restrict_to_up_acting,
  bool *history_les_bound) const
{
  ceph_assert(history_les_bound);
  /* See doc/dev/osd_internals/last_epoch_started.rst before attempting
   * to make changes to this process.  Also, make sure to update it
   * when you find bugs! */
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (!cct->_conf->osd_find_best_info_ignore_history_les &&
	max_last_epoch_started_found < i->second.history.last_epoch_started) {
      *history_les_bound = true;
      max_last_epoch_started_found = i->second.history.last_epoch_started;
    }
    if (!i->second.is_incomplete() &&
	max_last_epoch_started_found < i->second.last_epoch_started) {
      *history_les_bound = false;
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
  }
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found <= i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  if (min_last_update_acceptable == eversion_t::max())
    return infos.end();

  map<pg_shard_t, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<pg_shard_t, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    if (restrict_to_up_acting && !is_up(p->first) &&
	!is_acting(p->first))
      continue;
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disqualify anyone with a too old last_epoch_started
    if (p->second.last_epoch_started < max_last_epoch_started_found)
      continue;
    // Disqualify anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (pool.info.require_rollback()) {
      if (p->second.last_update > best->second.last_update)
	continue;
      if (p->second.last_update < best->second.last_update) {
	best = p;
	continue;
      }
    } else {
      if (p->second.last_update < best->second.last_update)
	continue;
      if (p->second.last_update > best->second.last_update) {
	best = p;
	continue;
      }
    }

    // Prefer longer tail
    if (p->second.log_tail > best->second.log_tail) {
      continue;
    } else if (p->second.log_tail < best->second.log_tail) {
      best = p;
      continue;
    }

    if (!p->second.has_missing() && best->second.has_missing()) {
      psdout(10) << __func__ << " prefer osd." << p->first
               << " because it is complete while best has missing"
               << dendl;
      best = p;
      continue;
    } else if (p->second.has_missing() && !best->second.has_missing()) {
      psdout(10) << __func__ << " skipping osd." << p->first
               << " because it has missing while best is complete"
               << dendl;
      continue;
    } else {
      // both are complete or have missing
      // fall through
    }

    // prefer current primary (usually the caller), all things being equal
    if (p->first == pg_whoami) {
      psdout(10) << "calc_acting prefer osd." << p->first
		 << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

void PeeringState::calc_ec_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  ostream &ss)
{
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  for (map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (uint8_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], shard_id_t(i)) << std::endl;
      want[i] = up[i];
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], shard_id_t(i))
	 << " and ";
      backfill->insert(pg_shard_t(up[i], shard_id_t(i)));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], shard_id_t(i)) << std::endl;
      want[i] = acting[i];
    } else if (!restrict_to_up_acting) {
      for (set<pg_shard_t>::iterator j = all_info_by_shard[shard_id_t(i)].begin();
	   j != all_info_by_shard[shard_id_t(i)].end();
	   ++j) {
	ceph_assert(j->shard == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << (int)i << std::endl;
    }
  }

  for (uint8_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], shard_id_t(i)));
    }
  }
  acting_backfill->insert(backfill->begin(), backfill->end());
  _want->swap(want);
}

/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
void PeeringState::calc_replicated_acting(
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
  ostream &ss)
{
  pg_shard_t auth_log_shard_id = auth_log_shard->first;

  ss << __func__ << " newest update on osd." << auth_log_shard_id
     << " with " << auth_log_shard->second
     << (restrict_to_up_acting ? " restrict_to_up_acting" : "") << std::endl;

  // select primary
  auto primary = all_info.find(up_primary);
  if (up.size() &&
      !primary->second.is_incomplete() &&
      primary->second.last_update >=
        auth_log_shard->second.log_tail) {
    if (HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS)) {
      auto approx_missing_objects =
        primary->second.stats.stats.sum.num_objects_missing;
      auto auth_version = auth_log_shard->second.last_update.version;
      auto primary_version = primary->second.last_update.version;
      if (auth_version > primary_version) {
        approx_missing_objects += auth_version - primary_version;
      } else {
        approx_missing_objects += primary_version - auth_version;
      }
      if ((uint64_t)approx_missing_objects >
          force_auth_primary_missing_objects) {
        primary = auth_log_shard;
        ss << "up_primary: " << up_primary << ") has approximate "
           << approx_missing_objects
           << "(>" << force_auth_primary_missing_objects <<") "
           << "missing objects, osd." << auth_log_shard_id
           << " selected as primary instead"
           << std::endl;
      } else {
        ss << "up_primary: " << up_primary << ") selected as primary"
           << std::endl;
      }
    } else {
      ss << "up_primary: " << up_primary << ") selected as primary" << std::endl;
    }
  } else {
    ceph_assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << __func__ << " primary is osd." << primary->first
     << " with " << primary->second << std::endl;
  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);

  /* We include auth_log_shard->second.log_tail because in GetLog,
   * we will request logs back to the min last_update over our
   * acting_backfill set, which will result in our log being extended
   * as far backwards as necessary to pick up any peers which can
   * be log recovered by auth_log_shard's log */
  eversion_t oldest_auth_log_entry =
    std::min(primary->second.log_tail, auth_log_shard->second.log_tail);

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds
  for (auto i : up) {
    pg_shard_t up_cand = pg_shard_t(i, shard_id_t::NO_SHARD);
    if (up_cand == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
        cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      backfill->insert(up_cand);
      acting_backfill->insert(up_cand);
    } else {
      want->push_back(i);
      acting_backfill->insert(up_cand);
      ss << " osd." << i << " (up) accepted " << cur_info << std::endl;
    }
    if (want->size() >= size) {
      break;
    }
  }

  if (want->size() >= size) {
    return;
  }

  std::vector<std::pair<eversion_t, int>> candidate_by_last_update;
  candidate_by_last_update.reserve(acting.size());
  // This no longer has backfill OSDs, but they are covered above.
  for (auto i : acting) {
    pg_shard_t acting_cand(i, shard_id_t::NO_SHARD);
    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << acting_cand << " (acting) REJECTED "
	 << cur_info << std::endl;
    } else {
      candidate_by_last_update.emplace_back(cur_info.last_update, i);
    }
  }

  auto sort_by_eversion =[](const std::pair<eversion_t, int> &lhs,
                            const std::pair<eversion_t, int> &rhs) {
    return lhs.first > rhs.first;
  };
  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);
  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (acting) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }

  if (restrict_to_up_acting) {
    return;
  }
  candidate_by_last_update.clear();
  candidate_by_last_update.reserve(all_info.size()); // overestimate but fine
  // continue to search stray to find more suitable peers
  for (auto &i : all_info) {
    // skip up osds we already considered above
    if (i.first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i.first.osd);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(
      acting.begin(), acting.end(), i.first.osd);
    if (acting_it != acting.end())
      continue;

    if (i.second.is_incomplete() ||
	i.second.last_update < oldest_auth_log_entry) {
      ss << " shard " << i.first << " (stray) REJECTED " << i.second
         << std::endl;
    } else {
      candidate_by_last_update.emplace_back(
        i.second.last_update, i.first.osd);
    }
  }

  if (candidate_by_last_update.empty()) {
    // save us some effort
    return;
  }

  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);

  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (stray) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }
}

bool PeeringState::recoverable_and_ge_min_size(const vector<int> &want) const
{
  unsigned num_want_acting = 0;
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      ++num_want_acting;
      have.insert(
        pg_shard_t(
          want[i],
          pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
  }
  // We go incomplete if below min_size for ec_pools since backfill
  // does not currently maintain rollbackability
  // Otherwise, we will go "peered", but not "active"
  if (num_want_acting < pool.info.min_size &&
      (pool.info.is_erasure() ||
       !cct->_conf->osd_allow_recovery_below_min_size)) {
    psdout(10) << __func__ << " failed, below min size" << dendl;
    return false;
  }

  /* Check whether we have enough acting shards to later perform recovery */
  if (!missing_loc.get_recoverable_predicate()(have)) {
    psdout(10) << __func__ << " failed, not recoverable" << dendl;
    return false;
  }

  return true;
}

void PeeringState::choose_async_recovery_ec(const map<pg_shard_t, pg_info_t> &all_info,
					    const pg_info_t &auth_info,
					    vector<int> *want,
					    set<pg_shard_t> *async_recovery) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (uint8_t i = 0; i < want->size(); ++i) {
    if ((*want)[i] == CRUSH_ITEM_NONE)
      continue;

    // Considering log entries to recover is accurate enough for
    // now. We could use minimum_to_decode_with_cost() later if
    // necessary.
    pg_shard_t shard_i((*want)[i], shard_id_t(i));
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // for ec pools we rollback all entries past the authoritative
    // last_update *before* activation. This is relatively inexpensive
    // compared to recovery, since it is purely local, so treat shards
    // past the authoritative last_update the same as those equal to it.
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects) >
	cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  psdout(20) << __func__ << " candidates by cost are: " << candidates_by_cost
	     << dendl;

  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    candidate_want[cur_shard.shard.id] = CRUSH_ITEM_NONE;
    if (recoverable_and_ge_min_size(candidate_want)) {
      want->swap(candidate_want);
      async_recovery->insert(cur_shard);
    }
  }
  psdout(20) << __func__ << " result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}

void PeeringState::choose_async_recovery_replicated(
  const map<pg_shard_t, pg_info_t> &all_info,
  const pg_info_t &auth_info,
  vector<int> *want,
  set<pg_shard_t> *async_recovery) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (auto osd_num : *want) {
    pg_shard_t shard_i(osd_num, shard_id_t::NO_SHARD);
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // use the approximate magnitude of the difference in length of
    // logs plus historical missing objects as the cost of recovery
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    } else {
      approx_missing_objects += candidate_version - auth_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects)  >
	cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  psdout(20) << __func__ << " candidates by cost are: " << candidates_by_cost
	     << dendl;
  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    if (want->size() <= pool.info.min_size) {
      break;
    }
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    for (auto it = candidate_want.begin(); it != candidate_want.end(); ++it) {
      if (*it == cur_shard.osd) {
        candidate_want.erase(it);
	want->swap(candidate_want);
	async_recovery->insert(cur_shard);
        break;
      }
    }
  }
  psdout(20) << __func__ << " result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}



/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 *
 * if restrict_to_up_acting=true, we filter out anything that's not in
 * up/acting.  in order to lift this restriction, we need to
 *  1) check whether it's worth switching the acting set any time we get
 *     a new pg info (not just here, when recovery finishes)
 *  2) check whether anything in want_acting went down on each new map
 *     (and, if so, calculate a new want_acting)
 *  3) remove the assertion in PG::PeeringState::Active::react(const AdvMap)
 * TODO!
 */
bool PeeringState::choose_acting(pg_shard_t &auth_log_shard_id,
				 bool restrict_to_up_acting,
				 bool *history_les_bound)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[pg_whoami] = info;

  if (cct->_conf->subsys.should_gather<dout_subsys, 10>()) {
    for (map<pg_shard_t, pg_info_t>::iterator p = all_info.begin();
         p != all_info.end();
         ++p) {
      psdout(10) << __func__ << " all_info osd." << p->first << " "
		 << p->second << dendl;
    }
  }

  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard =
    find_best_info(all_info, restrict_to_up_acting, history_les_bound);

  if (auth_log_shard == all_info.end()) {
    if (up != acting) {
      psdout(10) << __func__ << " no suitable info found (incomplete backfills?),"
		 << " reverting to up" << dendl;
      want_acting = up;
      vector<int> empty;
      pl->queue_want_pg_temp(empty);
    } else {
      psdout(10) << __func__ << " failed" << dendl;
      ceph_assert(want_acting.empty());
    }
    return false;
  }

  ceph_assert(!auth_log_shard->second.is_incomplete());
  auth_log_shard_id = auth_log_shard->first;

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  stringstream ss;
  if (pool.info.is_replicated())
    calc_replicated_acting(
      auth_log_shard,
      cct->_conf.get_val<uint64_t>(
        "osd_force_auth_primary_missing_objects"),
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      up_primary,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      get_osdmap(),
      ss);
  else
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      ss);
  psdout(10) << ss.str() << dendl;

  if (!recoverable_and_ge_min_size(want)) {
    want_acting.clear();
    return false;
  }

  set<pg_shard_t> want_async_recovery;
  if (HAVE_FEATURE(get_osdmap()->get_up_osd_features(), SERVER_MIMIC)) {
    if (pool.info.is_erasure()) {
      choose_async_recovery_ec(all_info, auth_log_shard->second, &want, &want_async_recovery);
    } else {
      choose_async_recovery_replicated(all_info, auth_log_shard->second, &want, &want_async_recovery);
    }
  }
  if (want != acting) {
    psdout(10) << __func__ << " want " << want << " != acting " << acting
	       << ", requesting pg_temp change" << dendl;
    want_acting = want;

    if (!cct->_conf->osd_debug_no_acting_change) {
      if (want_acting == up) {
	// There can't be any pending backfill if
	// want is the same as crush map up OSDs.
	ceph_assert(want_backfill.empty());
	vector<int> empty;
	pl->queue_want_pg_temp(empty);
      } else
	pl->queue_want_pg_temp(want);
    }
    return false;
  }
  want_acting.clear();
  acting_recovery_backfill = want_acting_backfill;
  psdout(10) << "acting_recovery_backfill is "
	     << acting_recovery_backfill << dendl;
  ceph_assert(
    backfill_targets.empty() ||
    backfill_targets == want_backfill);
  if (backfill_targets.empty()) {
    // Caller is GetInfo
    backfill_targets = want_backfill;
  }
  // Adding !needs_recovery() to let the async_recovery_targets reset after recovery is complete
  ceph_assert(
    async_recovery_targets.empty() ||
    async_recovery_targets == want_async_recovery ||
    !needs_recovery());
  if (async_recovery_targets.empty() || !needs_recovery()) {
    async_recovery_targets = want_async_recovery;
  }
  // Will not change if already set because up would have had to change
  // Verify that nothing in backfill is in stray_set
  for (set<pg_shard_t>::iterator i = want_backfill.begin();
      i != want_backfill.end();
      ++i) {
    ceph_assert(stray_set.find(*i) == stray_set.end());
  }
  psdout(10) << "choose_acting want=" << want << " backfill_targets="
           << want_backfill << " async_recovery_targets="
           << async_recovery_targets << dendl;
  return true;
}



/*------------ Peering State Machine----------------*/
#undef dout_prefix
#define dout_prefix (context< PeeringMachine >().dpp->gen_prefix(*_dout) \
                    << "state<" << get_state_name() << ">: ")
#undef psdout
#define psdout(x) ldout(context< PeeringMachine >().cct, x)

#define DECLARE_LOCALS                                  \
  PG *pg = context< PeeringMachine >().pg;              \
  std::ignore = pg;                                     \
  PeeringState *ps = context< PeeringMachine >().state; \
  std::ignore = ps;                                     \
  PeeringListener *pl = context< PeeringMachine >().pl; \
  std::ignore = pl;


/*------Crashed-------*/
PeeringState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Crashed")
{
  context< PeeringMachine >().log_enter(state_name);
  ceph_abort_msg("we got a bad state machine event");
}


/*------Initial-------*/
PeeringState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Initial")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::Initial::react(const MNotifyRec& notify)
{
  DECLARE_LOCALS
  ps->proc_replica_info(
    notify.from, notify.notify.info, notify.notify.epoch_sent);
  ps->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PeeringState::Initial::react(const MInfoRec& i)
{
  DECLARE_LOCALS
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PeeringState::Initial::react(const MLogRec& i)
{
  DECLARE_LOCALS
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PeeringState::Initial::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PeeringState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::Started::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const AdvMap& advmap)
{
  DECLARE_LOCALS
  psdout(10) << "Started advmap" << dendl;
  ps->check_full_transition(advmap.lastmap, advmap.osdmap);
  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should_restart_peering, transitioning to Reset"
		       << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  ps->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PeeringState::Started::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_started_latency, dur);
}

/*--------Reset---------*/
PeeringState::Reset::Reset(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Reset")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ps->flushes_in_progress = 0;
  ps->set_last_peering_reset();
}

boost::statechart::result
PeeringState::Reset::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const AdvMap& advmap)
{
  DECLARE_LOCALS
  psdout(10) << "Reset advmap" << dendl;

  ps->check_full_transition(advmap.lastmap, advmap.osdmap);

  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should restart peering, calling start_peering_interval again"
		       << dendl;
    ps->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< PeeringMachine >().get_cur_transaction());
  }
  ps->remove_down_peer_info(advmap.osdmap);
  ps->check_past_interval_bounds();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const ActMap&)
{
  DECLARE_LOCALS
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    context< PeeringMachine >().send_notify(
      ps->get_primary(),
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info),
      ps->past_intervals);
  }

  ps->update_heartbeat_peers();

  return transit< Started >();
}

boost::statechart::result PeeringState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PeeringState::Reset::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PeeringState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Start")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS
  if (ps->is_primary()) {
    psdout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    psdout(1) << "transitioning to Stray" << dendl;
    post_event(MakeStray());
  }
}

void PeeringState::Start::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PeeringState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  ceph_assert(ps->want_acting.empty());

  // set CREATING bit until we have peered for the first time.
  if (ps->info.history.last_epoch_started == 0) {
    ps->state_set(PG_STATE_CREATING);
    // use the history timestamp, which ultimately comes from the
    // monitor in the create case.
    utime_t t = ps->info.history.last_scrub_stamp;
    ps->info.stats.last_fresh = t;
    ps->info.stats.last_active = t;
    ps->info.stats.last_change = t;
    ps->info.stats.last_peered = t;
    ps->info.stats.last_clean = t;
    ps->info.stats.last_unstale = t;
    ps->info.stats.last_undegraded = t;
    ps->info.stats.last_fullsized = t;
    ps->info.stats.last_scrub_stamp = t;
    ps->info.stats.last_deep_scrub_stamp = t;
    ps->info.stats.last_clean_scrub_stamp = t;
  }
}

boost::statechart::result PeeringState::Primary::react(const MNotifyRec& notevt)
{
  DECLARE_LOCALS
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  ps->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(const ActMap&)
{
  DECLARE_LOCALS
  psdout(7) << "handle ActMap primary" << dendl;
  pl->publish_stats_to_osd();
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceRecovery&)
{
  DECLARE_LOCALS
  ps->set_force_recovery(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceRecovery&)
{
  DECLARE_LOCALS
  ps->set_force_recovery(false);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const RequestScrub& evt)
{
  DECLARE_LOCALS
  if (ps->is_primary()) {
    pl->scrub_requested(evt.deep, evt.repair);
    psdout(10) << "marking for scrub" << dendl;
  }
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceBackfill&)
{
  DECLARE_LOCALS
  ps->set_force_backfill(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceBackfill&)
{
  DECLARE_LOCALS
  ps->set_force_backfill(false);
  return discard_event();
}

void PeeringState::Primary::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  ps->want_acting.clear();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_primary_latency, dur);
  pl->clear_primary_state();
  ps->state_clear(PG_STATE_CREATING);
}

/*---------Peering--------*/
PeeringState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering"),
    history_les_bound(false)
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(ps->is_primary());
  ps->state_set(PG_STATE_PEERING);
}

boost::statechart::result PeeringState::Peering::react(const AdvMap& advmap)
{
  DECLARE_LOCALS
  psdout(10) << "Peering advmap" << dendl;
  if (prior_set.affected_by_map(*(advmap.osdmap), ps->dpp)) {
    psdout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  ps->adjust_need_up_thru(advmap.osdmap);

  return forward_event();
}

boost::statechart::result PeeringState::Peering::react(const QueryState& q)
{
  DECLARE_LOCALS

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  ps->past_intervals.dump(q.f);
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<pg_shard_t>::iterator p = prior_set.probe.begin();
       p != prior_set.probe.end();
       ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set.pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set.down.begin();
       p != prior_set.down.end();
       ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set.blocked_by.begin();
       p != prior_set.blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  if (history_les_bound) {
    q.f->open_array_section("peering_blocked_by_detail");
    q.f->open_object_section("item");
    q.f->dump_string("detail","peering_blocked_by_history_les_bound");
    q.f->close_section();
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

void PeeringState::Peering::exit()
{

  DECLARE_LOCALS
  psdout(10) << "Leaving Peering" << dendl;
  context< PeeringMachine >().log_exit(state_name, enter_time);
  ps->state_clear(PG_STATE_PEERING);
  pl->clear_probe_targets();

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PeeringState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Backfilling")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS
  ps->backfill_reserved = true;
  pl->on_backfill_reserved();
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_set(PG_STATE_BACKFILLING);
  pl->publish_stats_to_osd();
}

void PeeringState::Backfilling::backfill_release_reservations()
{
  DECLARE_LOCALS
  pl->cancel_local_background_io_reservation();
  for (set<pg_shard_t>::iterator it = ps->backfill_targets.begin();
       it != ps->backfill_targets.end();
       ++it) {
    ceph_assert(*it != ps->pg_whoami);
    pl->send_cluster_message(
      it->osd,
      new MBackfillReserve(
	MBackfillReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

void PeeringState::Backfilling::cancel_backfill()
{
  DECLARE_LOCALS
  backfill_release_reservations();
  pl->on_backfill_canceled();
}

boost::statechart::result
PeeringState::Backfilling::react(const Backfilled &c)
{
  backfill_release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Backfilling::react(const DeferBackfill &c)
{
  DECLARE_LOCALS

  psdout(10) << "defer backfill, retry delay " << c.delay << dendl;
  ps->state_set(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    c.delay);
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const UnfoundBackfill &c)
{
  DECLARE_LOCALS
  psdout(10) << "backfill has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_BACKFILL_UNFOUND);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevokedTooFull &)
{
  DECLARE_LOCALS

  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);

  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevoked &)
{
  DECLARE_LOCALS
  ps->state_set(PG_STATE_BACKFILL_WAIT);
  cancel_backfill();
  if (ps->needs_backfill()) {
    return transit<WaitLocalBackfillReserved>();
  } else {
    // raced with MOSDPGBackfill::OP_BACKFILL_FINISH, ignore
    return discard_event();
  }
}

void PeeringState::Backfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_BACKFILLING);
  ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PeeringState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().remote_shards_to_reserve_backfill.begin())
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->publish_stats_to_osd();
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS

  int64_t num_bytes = ps->info.stats.stats.sum.num_bytes;
  psdout(10) << __func__ << " num_bytes " << num_bytes << dendl;
  if (backfill_osd_it !=
      context< Active >().remote_shards_to_reserve_backfill.end()) {
    // The primary never backfills itself
    ceph_assert(*backfill_osd_it != ps->pg_whoami);
    pl->send_cluster_message(
      backfill_osd_it->osd,
      new MBackfillReserve(
	MBackfillReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid, backfill_osd_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_backfill_priority(),
        num_bytes,
        ps->peer_bytes[*backfill_osd_it]),
      ps->get_osdmap_epoch());
    ++backfill_osd_it;
  } else {
    ps->peer_bytes.clear();
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PeeringState::WaitRemoteBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremotebackfillreserved_latency, dur);
}

void PeeringState::WaitRemoteBackfillReserved::retry()
{
  DECLARE_LOCALS
  pl->cancel_local_background_io_reservation();

  // Send CANCEL to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end;
  begin = context< Active >().remote_shards_to_reserve_backfill.begin();
  end = context< Active >().remote_shards_to_reserve_backfill.end();
  ceph_assert(begin != end);
  for (it = begin; it != backfill_osd_it; ++it) {
    // The primary never backfills itself
    ceph_assert(*it != ps->pg_whoami);
    pl->send_cluster_message(
      it->osd,
      new MBackfillReserve(
	MBackfillReserve::RELEASE,
	spg_t(context< PeeringMachine >().spgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }

  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  pl->publish_stats_to_osd();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  retry();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRevoked &evt)
{
  retry();
  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PeeringState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->request_local_background_io_reservation(
    ps->get_backfill_priority(),
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalBackfillReserved()),
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferBackfill(0.0)));
  pl->publish_stats_to_osd();
}

void PeeringState::WaitLocalBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PeeringState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotBackfilling")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  ps->state_clear(PG_STATE_REPAIR);
  pl->publish_stats_to_osd();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteBackfillReserved &evt)
{
  return discard_event();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteReservationRejected &evt)
{
  return discard_event();
}

void PeeringState::NotBackfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  ps->state_clear(PG_STATE_BACKFILL_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notbackfilling_latency, dur);
}

/*----NotRecovering------*/
PeeringState::NotRecovering::NotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  pl->publish_stats_to_osd();
}

void PeeringState::NotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  ps->state_clear(PG_STATE_RECOVERY_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notrecovering_latency, dur);
}

/*---RepNotRecovering----*/
PeeringState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepNotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RejectRemoteReservation &evt)
{
  DECLARE_LOCALS
  ps->reject_reservation();
  post_event(RemoteReservationRejected());
  return discard_event();
}

void PeeringState::RepNotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PeeringState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  DECLARE_LOCALS
  pl->send_cluster_message(
    ps->primary.osd,
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

void PeeringState::RepWaitRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PeeringState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{

  DECLARE_LOCALS

  if (!pl->try_reserve_recovery_space(
	evt.primary_num_bytes, evt.local_num_bytes)) {
    post_event(RejectRemoteReservation());
  } else {
    // Use un-ec-adjusted bytes for stats.
    ps->info.stats.stats.sum.num_bytes = evt.local_num_bytes;

    PGPeeringEventRef preempt;
    if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
      // older peers will interpret preemption as TOOFULL
      preempt = std::make_shared<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
	RemoteBackfillPreempted());
    }
    pl->request_remote_recovery_reservation(
      evt.priority,
      std::make_shared<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
        RemoteBackfillReserved()),
      preempt);
  }
  return transit<RepWaitBackfillReserved>();
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestRecoveryPrio &evt)
{
  DECLARE_LOCALS

  // fall back to a local reckoning of priority of primary doesn't pass one
  // (pre-mimic compat)
  int prio = evt.priority ? evt.priority : ps->get_recovery_priority();

  PGPeeringEventRef preempt;
  if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
    // older peers can't handle this
    preempt = std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryPreempted());
  }

  pl->request_remote_recovery_reservation(
    prio,
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryReserved()),
    preempt);
  return transit<RepWaitRecoveryReserved>();
}

void PeeringState::RepWaitBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitbackfillreserved_latency, dur);
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS


  pl->send_cluster_message(
      ps->primary.osd,
      new MBackfillReserve(
	MBackfillReserve::GRANT,
	spg_t(ps->info.pgid.pgid, ps->primary.shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RejectRemoteReservation &evt)
{
  DECLARE_LOCALS
  ps->reject_reservation();
  post_event(RemoteReservationRejected());
  return discard_event();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationRejected &evt)
{
  DECLARE_LOCALS
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PeeringState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteRecoveryPreempted &)
{
  DECLARE_LOCALS


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    new MRecoveryReserve(
      MRecoveryReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const BackfillTooFull &)
{
  DECLARE_LOCALS


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    new MBackfillReserve(
      MBackfillReserve::TOOFULL,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteBackfillPreempted &)
{
  DECLARE_LOCALS


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

void PeeringState::RepRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reprecovering_latency, dur);
}

/*------Activating--------*/
PeeringState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Activating")
{
  context< PeeringMachine >().log_enter(state_name);
}

void PeeringState::Activating::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_activating_latency, dur);
}

PeeringState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  // Make sure all nodes that part of the recovery aren't full
  if (!ps->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      ps->get_osdmap()->check_full(ps->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return;
  }

  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  pl->request_local_background_io_reservation(
    ps->get_recovery_priority(),
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalRecoveryReserved()),
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferRecovery(0.0)));
  pl->publish_stats_to_osd();
}

boost::statechart::result
PeeringState::WaitLocalRecoveryReserved::react(const RecoveryTooFull &evt)
{
  DECLARE_LOCALS
  ps->state_set(PG_STATE_RECOVERY_TOOFULL);
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    ps->cct->_conf->osd_recovery_retry_interval);
  return transit<NotRecovering>();
}

void PeeringState::WaitLocalRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

PeeringState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    remote_recovery_reservation_it(context< Active >().remote_shards_to_reserve_recovery.begin())
{
  context< PeeringMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PeeringState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  DECLARE_LOCALS

  if (remote_recovery_reservation_it !=
      context< Active >().remote_shards_to_reserve_recovery.end()) {
    ceph_assert(*remote_recovery_reservation_it != ps->pg_whoami);
    pl->send_cluster_message(
      remote_recovery_reservation_it->osd,
      new MRecoveryReserve(
	MRecoveryReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid,
	      remote_recovery_reservation_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_recovery_priority()),
      ps->get_osdmap_epoch());
    ++remote_recovery_reservation_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PeeringState::WaitRemoteRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremoterecoveryreserved_latency, dur);
}

PeeringState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovering")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  ps->state_set(PG_STATE_RECOVERING);
  pl->on_recovery_reserved();
  ceph_assert(!ps->state_test(PG_STATE_ACTIVATING));
  pl->publish_stats_to_osd();
}

void PeeringState::Recovering::release_reservations(bool cancel)
{
  DECLARE_LOCALS
  ceph_assert(cancel || !ps->pg_log.get_missing().have_missing());

  // release remote reservations
  for (set<pg_shard_t>::const_iterator i =
	 context< Active >().remote_shards_to_reserve_recovery.begin();
        i != context< Active >().remote_shards_to_reserve_recovery.end();
        ++i) {
    if (*i == ps->pg_whoami) // skip myself
      continue;
    pl->send_cluster_message(
      i->osd,
      new MRecoveryReserve(
	MRecoveryReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, i->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

boost::statechart::result
PeeringState::Recovering::react(const AllReplicasRecovered &evt)
{
  DECLARE_LOCALS
  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  release_reservations();
  pl->cancel_local_background_io_reservation();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Recovering::react(const RequestBackfill &evt)
{
  DECLARE_LOCALS

  release_reservations();

  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  pl->cancel_local_background_io_reservation();
  pl->publish_stats_to_osd();
  // XXX: Is this needed?
  return transit<WaitLocalBackfillReserved>();
}

boost::statechart::result
PeeringState::Recovering::react(const DeferRecovery &evt)
{
  DECLARE_LOCALS
  if (!ps->state_test(PG_STATE_RECOVERING)) {
    // we may have finished recovery and have an AllReplicasRecovered
    // event queued to move us to the next state.
    psdout(10) << "got defer recovery but not recovering" << dendl;
    return discard_event();
  }
  psdout(10) << "defer recovery, retry delay " << evt.delay << dendl;
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    evt.delay);
  return transit<NotRecovering>();
}

boost::statechart::result
PeeringState::Recovering::react(const UnfoundRecovery &evt)
{
  DECLARE_LOCALS
  psdout(10) << "recovery has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_RECOVERY_UNFOUND);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  return transit<NotRecovering>();
}

void PeeringState::Recovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  ps->state_clear(PG_STATE_RECOVERING);
  pl->get_peering_perf().tinc(rs_recovering_latency, dur);
}

PeeringState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS

  ceph_assert(!ps->needs_recovery());

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED | UNDERSIZED is appropriate.
  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (ps->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid) <=
      ps->acting_recovery_backfill.size()) {
    ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
  }

  // adjust acting set?  (e.g. because backfill completed...)
  bool history_les_bound = false;
  if (ps->acting != ps->up && !ps->choose_acting(auth_log_shard,
						 true, &history_les_bound)) {
    ceph_assert(ps->want_acting.size());
  } else if (!ps->async_recovery_targets.empty()) {
    ps->choose_acting(auth_log_shard, true, &history_les_bound);
  }

  if (context< Active >().all_replicas_activated  &&
      ps->async_recovery_targets.empty())
    post_event(GoClean());
}

void PeeringState::Recovered::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_recovered_latency, dur);
}

PeeringState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Clean")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS

  if (ps->info.last_complete != ps->info.last_update) {
    ceph_abort();
  }
  Context *c = pg->finish_recovery();
  context< PeeringMachine >().get_cur_transaction()->register_on_commit(c);

  pg->try_mark_clean();
}

void PeeringState::Clean::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  ps->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_clean_latency, dur);
}

template <typename T>
set<pg_shard_t> unique_osd_shard_set(const pg_shard_t & skip, const T &in)
{
  set<int> osds_found;
  set<pg_shard_t> out;
  for (typename T::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (*i != skip && !osds_found.count(i->osd)) {
      osds_found.insert(i->osd);
      out.insert(*i);
    }
  }
  return out;
}

/*---------Active---------*/
PeeringState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active"),
    remote_shards_to_reserve_recovery(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->acting_recovery_backfill)),
    remote_shards_to_reserve_backfill(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->backfill_targets)),
    all_replicas_activated(false)
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS

  //ceph_assert(!ps->backfill_reserving);
  ceph_assert(!ps->backfill_reserved);
  ceph_assert(ps->is_primary());
  psdout(10) << "In Active, about to call activate" << dendl;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  pg->activate(*context< PeeringMachine >().get_cur_transaction(),
	       ps->get_osdmap_epoch(),
	       *context< PeeringMachine >().get_query_map(),
	       context< PeeringMachine >().get_info_map(),
	       context< PeeringMachine >().get_recovery_ctx());

  // everyone has to commit/ack before we are truly active
  ps->blocked_by.clear();
  for (set<pg_shard_t>::iterator p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (p->shard != ps->pg_whoami.shard) {
      ps->blocked_by.insert(p->shard);
    }
  }
  pl->publish_stats_to_osd();
  psdout(10) << "Activate Finished" << dendl;
}

boost::statechart::result PeeringState::Active::react(const AdvMap& advmap)
{
  DECLARE_LOCALS

  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "Active advmap interval change, fast return" << dendl;
    return forward_event();
  }
  psdout(10) << "Active advmap" << dendl;
  bool need_publish = false;

  pl->on_active_advmap(advmap.osdmap);
  if (ps->dirty_big_info) {
    // share updated purged_snaps to mgr/mon so that we (a) stop reporting
    // purged snaps and (b) perhaps share more snaps that we have purged
    // but didn't fit in pg_stat_t.
    need_publish = true;
    pg->share_pg_info();
  }

  for (size_t i = 0; i < ps->want_acting.size(); i++) {
    int osd = ps->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      ceph_assert(ps->is_acting(osd_with_shard) || ps->is_up(osd_with_shard));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(ps->info.pgid.pgid) !=
      ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid)) {
    if (ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid) <=
	ps->actingset.size()) {
      ps->state_clear(PG_STATE_UNDERSIZED);
    } else {
      ps->state_set(PG_STATE_UNDERSIZED);
    }
    // degraded changes will be detected by call from publish_stats_to_osd()
    need_publish = true;
  }

  // if we haven't reported our PG stats in a long time, do so now.
  if (ps->info.stats.reported_epoch + ps->cct->_conf->osd_pg_stat_report_interval_max < advmap.osdmap->get_epoch()) {
    psdout(20) << "reporting stats to osd after " << (advmap.osdmap->get_epoch() - ps->info.stats.reported_epoch)
		       << " epochs" << dendl;
    need_publish = true;
  }

  if (need_publish)
    pl->publish_stats_to_osd();

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const ActMap&)
{
  DECLARE_LOCALS
  psdout(10) << "Active: handling ActMap" << dendl;
  ceph_assert(ps->is_primary());

  pl->on_active_actmap();

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< PeeringMachine >().get_query_map());
  }

  uint64_t unfound = ps->missing_loc.num_unfound();
  if (unfound > 0 &&
      ps->all_unfound_are_queried_or_lost(ps->get_osdmap())) {
    if (ps->cct->_conf->osd_auto_mark_unfound_lost) {
      pl->get_clog().error() << context< PeeringMachine >().spgid.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically "
			    << "mark these objects lost but this feature is not yet implemented "
			    << "(osd_auto_mark_unfound_lost)";
    } else
      pl->get_clog().error() << context< PeeringMachine >().spgid.pgid << " has "
                             << unfound << " objects unfound and apparently lost";
  }

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const MNotifyRec& notevt)
{

  DECLARE_LOCALS
  ceph_assert(ps->is_primary());
  if (ps->peer_info.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already have info from that osd, ignoring"
		       << dendl;
  } else if (ps->peer_purged.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already purged that peer, ignoring"
		       << dendl;
  } else {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", calling proc_replica_info and discover_all_missing"
		       << dendl;
    ps->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< PeeringMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MTrim& trim)
{
  DECLARE_LOCALS
  ceph_assert(ps->is_primary());

  // peer is informing us of their last_complete_ondisk
  ldout(ps->cct,10) << " replica osd." << trim.from << " lcod " << trim.trim_to << dendl;
  ps->peer_last_complete_ondisk[pg_shard_t(trim.from, trim.shard)] = trim.trim_to;

  // trim log when the pg is recovered
  pg->calc_min_last_complete_ondisk();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS
  ceph_assert(ps->is_primary());

  ceph_assert(!ps->acting_recovery_backfill.empty());
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (ps->is_acting_recovery_backfill(infoevt.from) &&
      ps->peer_activated.count(infoevt.from) == 0) {
    psdout(10) << " peer osd." << infoevt.from
		       << " activated and committed" << dendl;
    ps->peer_activated.insert(infoevt.from);
    ps->blocked_by.erase(infoevt.from.shard);
    pl->publish_stats_to_osd();
    if (ps->peer_activated.size() == ps->acting_recovery_backfill.size()) {
      pg->all_activated_and_committed();
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MLogRec& logevt)
{
  DECLARE_LOCALS
  psdout(10) << "searching osd." << logevt.from
		     << " log for unfound items" << dendl;
  pg->proc_replica_log(
    logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  bool got_missing = pg->search_for_missing(
    ps->peer_info[logevt.from],
    ps->peer_missing[logevt.from],
    logevt.from,
    context< PeeringMachine >().get_recovery_ctx());
  // If there are missing AND we are "fully" active then start recovery now
  if (got_missing && ps->state_test(PG_STATE_ACTIVE)) {
    post_event(DoRecovery());
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const QueryState& q)
{
  DECLARE_LOCALS

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<pg_shard_t>::iterator p = ps->might_have_unfound.begin();
	 p != ps->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (ps->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (ps->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!ps->get_osdmap()->is_up(p->osd)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    pg->dump_recovery_info(q.f);
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const AllReplicasActivated &evt)
{

  DECLARE_LOCALS
  pg_t pgid = context< PeeringMachine >().spgid.pgid;

  all_replicas_activated = true;

  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_CREATING);
  ps->state_clear(PG_STATE_PREMERGE);

  bool merge_target;
  if (ps->pool.info.is_pending_merge(pgid, &merge_target)) {
    ps->state_set(PG_STATE_PEERED);
    ps->state_set(PG_STATE_PREMERGE);

    if (ps->actingset.size() != ps->get_osdmap()->get_pg_size(pgid)) {
      if (merge_target) {
	pg_t src = pgid;
	src.set_ps(ps->pool.info.get_pg_num_pending());
	assert(src.get_parent() == pgid);
	pg->osd->set_not_ready_to_merge_target(pgid, src);
      } else {
	pg->osd->set_not_ready_to_merge_source(pgid);
      }
    }
  } else if (ps->acting.size() < ps->pool.info.min_size) {
    ps->state_set(PG_STATE_PEERED);
  } else {
    ps->state_set(PG_STATE_ACTIVE);
  }

  if (ps->pool.info.has_flag(pg_pool_t::FLAG_CREATING)) {
    pg->osd->send_pg_created(pgid);
  }

  ps->info.history.last_epoch_started = ps->info.last_epoch_started;
  ps->info.history.last_interval_started = ps->info.last_interval_started;
  ps->dirty_info = true;

  pg->share_pg_info();
  pl->publish_stats_to_osd();

  pg->check_local();

  // waiters
  if (ps->flushes_in_progress == 0) {
    pg->requeue_ops(pg->waiting_for_peered);
  } else if (!pg->waiting_for_peered.empty()) {
    psdout(10) << __func__ << " flushes in progress, moving "
		       << pg->waiting_for_peered.size()
		       << " items to waiting_for_flush"
		       << dendl;
    ceph_assert(pg->waiting_for_flush.empty());
    pg->waiting_for_flush.swap(pg->waiting_for_peered);
  }

  pl->on_activate();

  return discard_event();
}

void PeeringState::Active::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);


  DECLARE_LOCALS
  pl->cancel_local_background_io_reservation();

  ps->blocked_by.clear();
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_DEGRADED);
  ps->state_clear(PG_STATE_UNDERSIZED);
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_active_latency, dur);
  pl->on_active_exit();
}

/*------ReplicaActive-----*/
PeeringState::ReplicaActive::ReplicaActive(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
}


boost::statechart::result PeeringState::ReplicaActive::react(
  const Activate& actevt) {
  DECLARE_LOCALS
  psdout(10) << "In ReplicaActive, about to call activate" << dendl;
  map<int, map<spg_t, pg_query_t> > query_map;
  pg->activate(*context< PeeringMachine >().get_cur_transaction(),
	       actevt.activation_epoch,
	       query_map, NULL, NULL);
  psdout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS
  pg->proc_primary_info(*context<PeeringMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MLogRec& logevt)
{
  DECLARE_LOCALS
  psdout(10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
  pg->merge_log(*t, logevt.msg->info, logevt.msg->log, logevt.from);
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MTrim& trim)
{
  DECLARE_LOCALS
  // primary is instructing us to trim
  ps->pg_log.trim(trim.trim_to, ps->info);
  ps->dirty_info = true;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const ActMap&)
{
  DECLARE_LOCALS
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    context< PeeringMachine >().send_notify(
      ps->get_primary(),
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info),
      ps->past_intervals);
  }
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(
  const MQuery& query)
{
  DECLARE_LOCALS
  pg->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return forward_event();
}

void PeeringState::ReplicaActive::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_replicaactive_latency, dur);
}

/*-------Stray---*/
PeeringState::Stray::Stray(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Stray")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS
  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(!ps->is_primary());

  if (!ps->get_osdmap()->have_pg_pool(ps->info.pgid.pgid.pool())) {
    ldout(ps->cct,10) << __func__ << " pool is deleted" << dendl;
    post_event(DeleteStart());
  } else {
    ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  }
}

boost::statechart::result PeeringState::Stray::react(const MLogRec& logevt)
{
  DECLARE_LOCALS
  MOSDPGLog *msg = logevt.msg.get();
  psdout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    ps->info = msg->info;
    pl->on_info_history_change();
    ps->dirty_info = true;
    ps->dirty_big_info = true;  // maybe.

    PG::PGLogEntryHandler rollbacker{pg, t};
    ps->pg_log.reset_backfill_claim_log(msg->log, &rollbacker);

    ps->pg_log.reset_backfill();
  } else {
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS
  psdout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (ps->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    ps->info.stats = infoevt.info.stats;
    ps->info.hit_set = infoevt.info.hit_set;
  }

  ceph_assert(infoevt.info.last_update == ps->info.last_update);
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  post_event(Activate(infoevt.info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MQuery& query)
{
  DECLARE_LOCALS
  pg->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::Stray::react(const ActMap&)
{
  DECLARE_LOCALS
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    context< PeeringMachine >().send_notify(
      ps->get_primary(),
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info),
      ps->past_intervals);
  }
  return discard_event();
}

void PeeringState::Stray::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_stray_latency, dur);
}


/*--------ToDelete----------*/
PeeringState::ToDelete::ToDelete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  pg->osd->logger->inc(l_osd_pg_removing);
}

void PeeringState::ToDelete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  // note: on a successful removal, this path doesn't execute. see
  // _delete_some().
  pg->osd->logger->dec(l_osd_pg_removing);

  pl->cancel_local_background_io_reservation();
}

/*----WaitDeleteReserved----*/
PeeringState::WaitDeleteReserved::WaitDeleteReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history,
	       "Started/ToDelete/WaitDeleteReseved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  context< ToDelete >().priority = ps->get_delete_priority();

  pl->cancel_local_background_io_reservation();
  pl->request_local_background_io_reservation(
    context<ToDelete>().priority,
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteReserved()),
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteInterrupted()));
}

boost::statechart::result PeeringState::ToDelete::react(
  const ActMap& evt)
{
  DECLARE_LOCALS
  if (ps->get_delete_priority() != priority) {
    psdout(10) << __func__ << " delete priority changed, resetting"
		   << dendl;
    return transit<ToDelete>();
  }
  return discard_event();
}

void PeeringState::WaitDeleteReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
}

/*----Deleting-----*/
PeeringState::Deleting::Deleting(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete/Deleting")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS
  ps->deleting = true;
  ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
  pg->on_removal(t);
  t->register_on_commit(new PG::C_DeleteMore(pg, ps->get_osdmap_epoch()));
}

boost::statechart::result PeeringState::Deleting::react(
  const DeleteSome& evt)
{
  DECLARE_LOCALS
  pg->_delete_some(context<PeeringMachine>().get_cur_transaction());
  return discard_event();
}

void PeeringState::Deleting::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  ps->deleting = false;
  pl->cancel_local_background_io_reservation();
}

/*--------GetInfo---------*/
PeeringState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetInfo")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS
  ps->check_past_interval_bounds();
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  ceph_assert(ps->blocked_by.empty());

  prior_set = ps->build_prior();

  pg->reset_min_peer_features();
  get_infos();
  if (prior_set.pg_down) {
    post_event(IsDown());
  } else if (peer_info_requested.empty()) {
    post_event(GotInfo());
  }
}

void PeeringState::GetInfo::get_infos()
{
  DECLARE_LOCALS
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  ps->blocked_by.clear();
  for (set<pg_shard_t>::const_iterator it = prior_set.probe.begin();
       it != prior_set.probe.end();
       ++it) {
    pg_shard_t peer = *it;
    if (peer == ps->pg_whoami) {
      continue;
    }
    if (ps->peer_info.count(peer)) {
      psdout(10) << " have osd." << peer << " info " << ps->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      psdout(10) << " already requested info from osd." << peer << dendl;
      ps->blocked_by.insert(peer.osd);
    } else if (!ps->get_osdmap()->is_up(peer.osd)) {
      psdout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      psdout(10) << " querying info from osd." << peer << dendl;
      context< PeeringMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 it->shard, ps->pg_whoami.shard,
			 ps->info.history,
			 ps->get_osdmap_epoch()));
      peer_info_requested.insert(peer);
      ps->blocked_by.insert(peer.osd);
    }
  }

  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetInfo::react(const MNotifyRec& infoevt)
{

  DECLARE_LOCALS

  set<pg_shard_t>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) {
    peer_info_requested.erase(p);
    ps->blocked_by.erase(infoevt.from.osd);
  }

  epoch_t old_start = ps->info.history.last_epoch_started;
  if (ps->proc_replica_info(
	infoevt.from, infoevt.notify.info, infoevt.notify.epoch_sent)) {
    // we got something new ...
    PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
    if (old_start < ps->info.history.last_epoch_started) {
      psdout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      prior_set = ps->build_prior();

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<pg_shard_t>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set.probe.count(*p) == 0) {
	  psdout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }
    psdout(20) << "Adding osd: " << infoevt.from.osd << " peer features: "
		       << hex << infoevt.features << dec << dendl;
    pg->apply_peer_features(infoevt.features);

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set.pg_down) {
      psdout(20) << "Common peer features: " << hex << pg->get_min_peer_features() << dec << dendl;
      psdout(20) << "Common acting features: " << hex << pg->get_min_acting_features() << dec << dendl;
      psdout(20) << "Common upacting features: " << hex << pg->get_min_upacting_features() << dec << dendl;
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::GetInfo::react(const QueryState& q)
{
  DECLARE_LOCALS
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<pg_shard_t>::iterator p = peer_info_requested.begin();
       p != peer_info_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      ps->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PeeringState::GetInfo::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getinfo_latency, dur);
  ps->blocked_by.clear();
}

/*------GetLog------------*/
PeeringState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< PeeringMachine >().state_history,
      "Started/Primary/Peering/GetLog"),
    msg(0)
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS

  // adjust acting?
  if (!ps->choose_acting(auth_log_shard, false,
			 &context< Peering >().history_les_bound)) {
    if (!ps->want_acting.empty()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (auth_log_shard == ps->pg_whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = ps->peer_info[auth_log_shard];

  // am i broken?
  if (ps->info.last_update < best.log_tail) {
    psdout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = ps->info.last_update;
  ceph_assert(!ps->acting_recovery_backfill.empty());
  for (set<pg_shard_t>::iterator p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (*p == ps->pg_whoami) continue;
    pg_info_t& ri = ps->peer_info[*p];
    if (ri.last_update < ps->info.log_tail && ri.last_update >= best.log_tail &&
        ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  psdout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<PeeringMachine>().send_query(
    auth_log_shard,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, ps->pg_whoami.shard,
      request_log_from, ps->info.history,
      ps->get_osdmap_epoch()));

  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(auth_log_shard.osd);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    psdout(10) << "GetLog: auth_log_shard osd."
		       << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PeeringState::GetLog::react(const MLogRec& logevt)
{
  ceph_assert(!msg);
  if (logevt.from != auth_log_shard) {
    psdout(10) << "GetLog: discarding log from "
		       << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }
  psdout(10) << "GetLog: received master log from osd."
		     << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result PeeringState::GetLog::react(const GotLog&)
{

  DECLARE_LOCALS
  psdout(10) << "leaving GetLog" << dendl;
  if (msg) {
    psdout(10) << "processing master log" << dendl;
    pg->proc_master_log(*context<PeeringMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing,
			auth_log_shard);
  }
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  return transit< GetMissing >();
}

boost::statechart::result PeeringState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_stream("auth_log_shard") << auth_log_shard;
  q.f->close_section();
  return forward_event();
}

void PeeringState::GetLog::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getlog_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitActingChange--------*/
PeeringState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/WaitActingChange")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitActingChange::react(const AdvMap& advmap)
{
  DECLARE_LOCALS
  OSDMapRef osdmap = advmap.osdmap;

  psdout(10) << "verifying no want_acting " << ps->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = ps->want_acting.begin(); p != ps->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      psdout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MLogRec& logevt)
{
  psdout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MInfoRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MNotifyRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void PeeringState::WaitActingChange::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitactingchange_latency, dur);
}

/*------Down--------*/
PeeringState::Down::Down(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Down")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_DOWN);

  auto &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

void PeeringState::Down::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS

  ps->state_clear(PG_STATE_DOWN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_down_latency, dur);

  ps->blocked_by.clear();
}

boost::statechart::result PeeringState::Down::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment",
		   "not enough up instances of this PG to go active");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Down::react(const MNotifyRec& infoevt)
{
  DECLARE_LOCALS

  ceph_assert(ps->is_primary());
  epoch_t old_start = ps->info.history.last_epoch_started;
  if (!ps->peer_info.count(infoevt.from) &&
      ps->get_osdmap()->has_been_up_since(infoevt.from.osd, infoevt.notify.epoch_sent)) {
    ps->update_history(infoevt.notify.info.history);
  }
  // if we got something new to make pg escape down state
  if (ps->info.history.last_epoch_started > old_start) {
      psdout(10) << " last_epoch_started moved forward, re-enter getinfo" << dendl;
    ps->state_clear(PG_STATE_DOWN);
    ps->state_set(PG_STATE_PEERING);
    return transit< GetInfo >();
  }

  return discard_event();
}


/*------Incomplete--------*/
PeeringState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Incomplete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_INCOMPLETE);

  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::Incomplete::react(const AdvMap &advmap) {
  DECLARE_LOCALS
  int64_t poolnum = ps->info.pgid.pool();

  // Reset if min_size turn smaller than previous value, pg might now be able to go active
  if (!advmap.osdmap->have_pg_pool(poolnum) ||
      advmap.lastmap->get_pools().find(poolnum)->second.min_size >
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PeeringState::Incomplete::react(const MNotifyRec& notevt) {
  DECLARE_LOCALS
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  if (ps->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent)) {
    // We got something new, try again!
    return transit< GetLog >();
  } else {
    return discard_event();
  }
}

boost::statechart::result PeeringState::Incomplete::react(
  const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "not enough complete instances of this PG");
  q.f->close_section();
  return forward_event();
}

void PeeringState::Incomplete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS

  ps->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_incomplete_latency, dur);

  ps->blocked_by.clear();
}

/*------GetMissing--------*/
PeeringState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetMissing")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS
  ceph_assert(!ps->acting_recovery_backfill.empty());
  eversion_t since;
  for (set<pg_shard_t>::iterator i = ps->acting_recovery_backfill.begin();
       i != ps->acting_recovery_backfill.end();
       ++i) {
    if (*i == ps->get_primary()) continue;
    const pg_info_t& pi = ps->peer_info[*i];
    // reset this so to make sure the pg_missing_t is initialized and
    // has the correct semantics even if we don't need to get a
    // missing set from a shard. This way later additions due to
    // lost+unfound delete work properly.
    ps->peer_missing[*i].may_include_deletes = !pg->perform_deletes_during_peering();

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < ps->pg_log.get_tail()) {
      psdout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      psdout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == ps->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      psdout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    since.epoch = pi.last_epoch_started;
    ceph_assert(pi.last_update >= ps->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      psdout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< PeeringMachine >().send_query(
	*i,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, ps->pg_whoami.shard,
	  since, ps->info.history,
	  ps->get_osdmap_epoch()));
    } else {
      psdout(10) << " requesting fulllog+missing from osd." << *i
			 << " (want since " << since << " < log.tail "
			 << pi.log_tail << ")" << dendl;
      context< PeeringMachine >().send_query(
	*i, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, ps->pg_whoami.shard,
	  ps->info.history, ps->get_osdmap_epoch()));
    }
    peer_missing_requested.insert(*i);
    ps->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    if (ps->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate(ps->get_osdmap_epoch()));
  } else {
    pl->publish_stats_to_osd();
  }
}

boost::statechart::result PeeringState::GetMissing::react(const MLogRec& logevt)
{
  DECLARE_LOCALS

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);

  if (peer_missing_requested.empty()) {
    if (ps->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
    } else {
      psdout(10) << "Got last missing, don't need missing "
			 << "posting Activate" << dendl;
      post_event(Activate(ps->get_osdmap_epoch()));
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::GetMissing::react(const QueryState& q)
{
  DECLARE_LOCALS
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<pg_shard_t>::iterator p = peer_missing_requested.begin();
       p != peer_missing_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      ps->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PeeringState::GetMissing::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getmissing_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitUpThru--------*/
PeeringState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/WaitUpThru")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitUpThru::react(const ActMap& am)
{
  DECLARE_LOCALS
  if (!ps->need_up_thru) {
    post_event(Activate(ps->get_osdmap_epoch()));
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const MLogRec& logevt)
{
  DECLARE_LOCALS
  psdout(10) << "Noting missing from osd." << logevt.from << dendl;
  ps->peer_missing[logevt.from].claim(logevt.msg->missing);
  ps->peer_info[logevt.from] = logevt.msg->info;
  return discard_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void PeeringState::WaitUpThru::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitupthru_latency, dur);
}

/*----PeeringState::PeeringMachine Methods-----*/
#undef dout_prefix
#define dout_prefix pg->gen_prefix(*_dout)

void PeeringState::PeeringMachine::log_enter(const char *state_name)
{
  DECLARE_LOCALS
  psdout(5) << "enter " << state_name << dendl;
  pg->osd->pg_recovery_stats.log_enter(state_name);
}

void PeeringState::PeeringMachine::log_exit(const char *state_name, utime_t enter_time)
{
  DECLARE_LOCALS
  utime_t dur = ceph_clock_now() - enter_time;
  psdout(5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, ceph_clock_now() - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix ((debug_pg ? debug_pg->gen_prefix(*_dout) : *_dout) << " PriorSet: ")

void PeeringState::start_handle(PeeringCtx *new_ctx) {
  ceph_assert(!rctx);
  ceph_assert(!orig_ctx);
  orig_ctx = new_ctx;
  if (new_ctx) {
    if (messages_pending_flush) {
      rctx = PeeringCtx(*messages_pending_flush, *new_ctx);
    } else {
      rctx = *new_ctx;
    }
    rctx->start_time = ceph_clock_now();
  }
}

void PeeringState::begin_block_outgoing() {
  ceph_assert(!messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = BufferedRecoveryMessages();
  rctx = PeeringCtx(*messages_pending_flush, *orig_ctx);
}

void PeeringState::clear_blocked_outgoing() {
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_block_outgoing() {
  ceph_assert(messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);

  rctx = PeeringCtx(*orig_ctx);
  rctx->accept_buffered_messages(*messages_pending_flush);
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_handle() {
  if (rctx) {
    utime_t dur = ceph_clock_now() - rctx->start_time;
    machine.event_time += dur;
  }

  machine.event_count++;
  rctx = boost::optional<PeeringCtx>();
  orig_ctx = NULL;
}
