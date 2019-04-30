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
	osdmap.get(),
	lastmap.get(),
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
      osdmap.get(),
      lastmap.get(),
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

int PeeringState::clamp_recovery_priority(int priority, int pool_recovery_priority, int max)
{
  static_assert(OSD_RECOVERY_PRIORITY_MIN < OSD_RECOVERY_PRIORITY_MAX, "Invalid priority range");
  static_assert(OSD_RECOVERY_PRIORITY_MIN >= 0, "Priority range must match unsigned type");

  ceph_assert(max <= OSD_RECOVERY_PRIORITY_MAX);

  // User can't set this too high anymore, but might be a legacy value
  if (pool_recovery_priority > OSD_POOL_PRIORITY_MAX)
    pool_recovery_priority = OSD_POOL_PRIORITY_MAX;
  if (pool_recovery_priority < OSD_POOL_PRIORITY_MIN)
    pool_recovery_priority = OSD_POOL_PRIORITY_MIN;
  // Shift range from min to max to 0 to max - min
  pool_recovery_priority += (0 - OSD_POOL_PRIORITY_MIN);
  ceph_assert(pool_recovery_priority >= 0 && pool_recovery_priority <= (OSD_POOL_PRIORITY_MAX - OSD_POOL_PRIORITY_MIN));

  priority += pool_recovery_priority;

  // Clamp to valid range
  if (priority > max) {
    return max;
  } else if (priority < OSD_RECOVERY_PRIORITY_MIN) {
    return OSD_RECOVERY_PRIORITY_MIN;
  } else {
    return priority;
  }
}

unsigned PeeringState::get_recovery_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_RECOVERY_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_RECOVERY) {
    ret = OSD_RECOVERY_PRIORITY_FORCED;
  } else {
    // XXX: This priority boost isn't so much about inactive, but about data-at-risk
    if (is_degraded() && info.stats.avail_no_missing.size() < pool.info.min_size) {
      base = OSD_RECOVERY_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - info.stats.avail_no_missing.size());
    }

    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, max_prio_map[base]);
  }
  psdout(20) << __func__ << " recovery priority is " << ret << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_backfill_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_BACKFILL_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_BACKFILL) {
    ret = OSD_BACKFILL_PRIORITY_FORCED;
  } else {
    if (acting.size() < pool.info.min_size) {
      base = OSD_BACKFILL_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - acting.size());

    } else if (is_undersized()) {
      // undersized: OSD_BACKFILL_DEGRADED_PRIORITY_BASE + num missing replicas
      ceph_assert(pool.info.size > actingset.size());
      base = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
      ret = base + (pool.info.size - actingset.size());

    } else if (is_degraded()) {
      // degraded: baseline degraded
      base = ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
    }

    // Adjust with pool's recovery priority
    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, max_prio_map[base]);
  }

  psdout(20) << __func__ << " backfill priority is " << ret << dendl;
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
  pg->past_intervals.dump(q.f);
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
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();

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
  pg->queue_recovery();
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
  if (!pg->waiting_on_backfill.empty()) {
    pg->waiting_on_backfill.clear();
    pg->finish_recovery_op(hobject_t::get_max());
  }
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
  pg->state_set(PG_STATE_BACKFILL_UNFOUND);
  pg->state_clear(PG_STATE_BACKFILLING);
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
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  cancel_backfill();
  if (pg->needs_backfill()) {
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
  pg->backfill_reserving = false;
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
  pg->state_clear(PG_STATE_REPAIR);
  pg->publish_stats_to_osd();
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
  pg->state_clear(PG_STATE_BACKFILL_UNFOUND);
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
  pg->publish_stats_to_osd();
}

void PeeringState::NotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS
  pg->state_clear(PG_STATE_RECOVERY_UNFOUND);
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
  pg->reject_reservation();
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
  pg->clear_reserved_num_bytes();

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

// Compute pending backfill data
static int64_t pending_backfill(CephContext *cct, int64_t bf_bytes, int64_t local_bytes)
{
    lgeneric_dout(cct, 20) << __func__ << " Adjust local usage " << (local_bytes >> 10) << "KiB"
		               << " primary usage " << (bf_bytes >> 10) << "KiB" << dendl;
    return std::max((int64_t)0, bf_bytes - local_bytes);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{

  DECLARE_LOCALS
  // Use tentative_bacfill_full() to make sure enough
  // space is available to handle target bytes from primary.

  // TODO: If we passed num_objects from primary we could account for
  // an estimate of the metadata overhead.

  // TODO: If we had compressed_allocated and compressed_original from primary
  // we could compute compression ratio and adjust accordingly.

  // XXX: There is no way to get omap overhead and this would only apply
  // to whatever possibly different partition that is storing the database.

  // update_osd_stat() from heartbeat will do this on a new
  // statfs using pg->primary_num_bytes.
  uint64_t pending_adjustment = 0;
  int64_t primary_num_bytes = evt.primary_num_bytes;
  int64_t local_num_bytes = evt.local_num_bytes;
  if (primary_num_bytes) {
    // For erasure coded pool overestimate by a full stripe per object
    // because we don't know how each objected rounded to the nearest stripe
    if (pg->pool.info.is_erasure()) {
      primary_num_bytes /= (int)pg->get_pgbackend()->get_ec_data_chunk_count();
      primary_num_bytes += pg->get_pgbackend()->get_ec_stripe_chunk_size() * pg->info.stats.stats.sum.num_objects;
      local_num_bytes /= (int)pg->get_pgbackend()->get_ec_data_chunk_count();
      local_num_bytes += pg->get_pgbackend()->get_ec_stripe_chunk_size() * pg->info.stats.stats.sum.num_objects;
    }
    pending_adjustment = pending_backfill(
      context< PeeringMachine >().cct,
      primary_num_bytes,
      local_num_bytes);
    psdout(10) << __func__ << " primary_num_bytes " << (primary_num_bytes >> 10) << "KiB"
                       << " local " << (local_num_bytes >> 10) << "KiB"
                       << " pending_adjustments " << (pending_adjustment >> 10) << "KiB"
                       << dendl;
  }
  // This lock protects not only the stats OSDService but also setting the pg primary_num_bytes
  // That's why we don't immediately unlock
  Mutex::Locker l(pg->osd->stat_lock);
  osd_stat_t cur_stat = pg->osd->osd_stat;
  if (pg->cct->_conf->osd_debug_reject_backfill_probability > 0 &&
      (rand()%1000 < (pg->cct->_conf->osd_debug_reject_backfill_probability*1000.0))) {
    psdout(10) << "backfill reservation rejected: failure injection"
		       << dendl;
    post_event(RejectRemoteReservation());
  } else if (!pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation &&
      pg->osd->tentative_backfill_full(pg, pending_adjustment, cur_stat)) {
    psdout(10) << "backfill reservation rejected: backfill full"
		       << dendl;
    post_event(RejectRemoteReservation());
  } else {
    // Don't reserve space if skipped reservation check, this is used
    // to test the other backfill full check AND in case a corruption
    // of num_bytes requires ignoring that value and trying the
    // backfill anyway.
    if (primary_num_bytes && !pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation)
      pg->set_reserved_num_bytes(primary_num_bytes, local_num_bytes);
    else
      pg->clear_reserved_num_bytes();
    // Use un-ec-adjusted bytes for stats.
    pg->info.stats.stats.sum.num_bytes = evt.local_num_bytes;

    PGPeeringEventRef preempt;
    if (HAVE_FEATURE(pg->upacting_features, RECOVERY_RESERVATION_2)) {
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
  pg->reject_reservation();
  post_event(RemoteReservationRejected());
  return discard_event();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationRejected &evt)
{
  DECLARE_LOCALS
  pg->clear_reserved_num_bytes();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS
  pg->clear_reserved_num_bytes();

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


  pg->clear_reserved_num_bytes();
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


  pg->clear_reserved_num_bytes();
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


  pg->clear_reserved_num_bytes();
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
  pg->clear_reserved_num_bytes();

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
      ps->pg->osd->check_osdmap_full(ps->acting_recovery_backfill)) {
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
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_TOOFULL);
  pg->state_set(PG_STATE_RECOVERING);
  ceph_assert(!pg->state_test(PG_STATE_ACTIVATING));
  pg->publish_stats_to_osd();
  pg->queue_recovery();
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
  pg->state_clear(PG_STATE_RECOVERING);
  pl->get_peering_perf().tinc(rs_recovering_latency, dur);
}

PeeringState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS

  ceph_assert(!pg->needs_recovery());

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED | UNDERSIZED is appropriate.
  ceph_assert(!pg->acting_recovery_backfill.empty());
  if (pg->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid) <=
      pg->acting_recovery_backfill.size()) {
    pg->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    pg->publish_stats_to_osd();
  }

  // adjust acting set?  (e.g. because backfill completed...)
  bool history_les_bound = false;
  if (pg->acting != pg->up && !pg->choose_acting(auth_log_shard,
						 true, &history_les_bound)) {
    ceph_assert(pg->want_acting.size());
  } else if (!pg->async_recovery_targets.empty()) {
    pg->choose_acting(auth_log_shard, true, &history_les_bound);
  }

  if (context< Active >().all_replicas_activated  &&
      pg->async_recovery_targets.empty())
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

  if (pg->info.last_complete != pg->info.last_update) {
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
  pg->state_clear(PG_STATE_CLEAN);
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
	context< PeeringMachine >().pg->pg_whoami,
	context< PeeringMachine >().pg->acting_recovery_backfill)),
    remote_shards_to_reserve_backfill(
      unique_osd_shard_set(
	context< PeeringMachine >().pg->pg_whoami,
	context< PeeringMachine >().pg->backfill_targets)),
    all_replicas_activated(false)
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS

  //ceph_assert(!ps->backfill_reserving);
  ceph_assert(!ps->backfill_reserved);
  ceph_assert(pg->is_primary());
  psdout(10) << "In Active, about to call activate" << dendl;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  pg->activate(*context< PeeringMachine >().get_cur_transaction(),
	       pg->get_osdmap_epoch(),
	       *context< PeeringMachine >().get_query_map(),
	       context< PeeringMachine >().get_info_map(),
	       context< PeeringMachine >().get_recovery_ctx());

  // everyone has to commit/ack before we are truly active
  pg->blocked_by.clear();
  for (set<pg_shard_t>::iterator p = pg->acting_recovery_backfill.begin();
       p != pg->acting_recovery_backfill.end();
       ++p) {
    if (p->shard != pg->pg_whoami.shard) {
      pg->blocked_by.insert(p->shard);
    }
  }
  pg->publish_stats_to_osd();
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

  if (advmap.osdmap->require_osd_release >= CEPH_RELEASE_MIMIC) {
    const auto& new_removed_snaps = advmap.osdmap->get_new_removed_snaps();
    auto i = new_removed_snaps.find(pg->info.pgid.pool());
    if (i != new_removed_snaps.end()) {
      bool bad = false;
      for (auto j : i->second) {
	if (pg->snap_trimq.intersects(j.first, j.second)) {
	  decltype(pg->snap_trimq) added, overlap;
	  added.insert(j.first, j.second);
	  overlap.intersection_of(pg->snap_trimq, added);
	  if (pg->last_require_osd_release < CEPH_RELEASE_MIMIC) {
	    lderr(pg->cct) << __func__ << " removed_snaps already contains "
			   << overlap << ", but this is the first mimic+ osdmap,"
			   << " so it's expected" << dendl;
	  } else {
	    lderr(pg->cct) << __func__ << " removed_snaps already contains "
			   << overlap << dendl;
	    bad = true;
	  }
	  pg->snap_trimq.union_of(added);
	} else {
	  pg->snap_trimq.insert(j.first, j.second);
	}
      }
      if (pg->last_require_osd_release < CEPH_RELEASE_MIMIC) {
	// at upgrade, we report *all* previously removed snaps as removed in
	// the first mimic epoch.  remove the ones we previously divined were
	// removed (and subsequently purged) from the trimq.
	lderr(pg->cct) << __func__ << " first mimic map, filtering purged_snaps"
		       << " from new removed_snaps" << dendl;
	pg->snap_trimq.subtract(pg->info.purged_snaps);
      }
      ldout(pg->cct,10) << __func__ << " new removed_snaps " << i->second
			<< ", snap_trimq now " << pg->snap_trimq << dendl;
      ceph_assert(!bad || !pg->cct->_conf->osd_debug_verify_cached_snaps);
      pg->dirty_info = true;
      pg->dirty_big_info = true;
    }

    const auto& new_purged_snaps = advmap.osdmap->get_new_purged_snaps();
    auto j = new_purged_snaps.find(pg->info.pgid.pool());
    if (j != new_purged_snaps.end()) {
      bool bad = false;
      for (auto k : j->second) {
	if (!pg->info.purged_snaps.contains(k.first, k.second)) {
	  decltype(pg->info.purged_snaps) rm, overlap;
	  rm.insert(k.first, k.second);
	  overlap.intersection_of(pg->info.purged_snaps, rm);
	  lderr(pg->cct) << __func__ << " purged_snaps does not contain "
			 << rm << ", only " << overlap << dendl;
	  pg->info.purged_snaps.subtract(overlap);
	  // This can currently happen in the normal (if unlikely) course of
	  // events.  Because adding snaps to purged_snaps does not increase
	  // the pg version or add a pg log entry, we don't reliably propagate
	  // purged_snaps additions to other OSDs.
	  // One example:
	  //  - purge S
	  //  - primary and replicas update purged_snaps
	  //  - no object updates
	  //  - pg mapping changes, new primary on different node
	  //  - new primary pg version == eversion_t(), so info is not
	  //    propagated.
	  //bad = true;
	} else {
	  pg->info.purged_snaps.erase(k.first, k.second);
	}
      }
      ldout(pg->cct,10) << __func__ << " new purged_snaps " << j->second
			<< ", now " << pg->info.purged_snaps << dendl;
      ceph_assert(!bad || !pg->cct->_conf->osd_debug_verify_cached_snaps);
      pg->dirty_info = true;
      pg->dirty_big_info = true;
    }
    if (pg->dirty_big_info) {
      // share updated purged_snaps to mgr/mon so that we (a) stop reporting
      // purged snaps and (b) perhaps share more snaps that we have purged
      // but didn't fit in pg_stat_t.
      need_publish = true;
      pg->share_pg_info();
    }
  } else if (!pg->pool.newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool.newly_removed_snaps);
    psdout(10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
    pg->dirty_big_info = true;
  }

  for (size_t i = 0; i < pg->want_acting.size(); i++) {
    int osd = pg->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      ceph_assert(pg->is_acting(osd_with_shard) || pg->is_up(osd_with_shard));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(context< PeeringMachine >().spgid.pgid) !=
      pg->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid)) {
    if (pg->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid) <=
	pg->actingset.size()) {
      pg->state_clear(PG_STATE_UNDERSIZED);
    } else {
      pg->state_set(PG_STATE_UNDERSIZED);
    }
    // degraded changes will be detected by call from publish_stats_to_osd()
    need_publish = true;
  }

  // if we haven't reported our PG stats in a long time, do so now.
  if (pg->info.stats.reported_epoch + pg->cct->_conf->osd_pg_stat_report_interval_max < advmap.osdmap->get_epoch()) {
    psdout(20) << "reporting stats to osd after " << (advmap.osdmap->get_epoch() - pg->info.stats.reported_epoch)
		       << " epochs" << dendl;
    need_publish = true;
  }

  if (need_publish)
    pg->publish_stats_to_osd();

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const ActMap&)
{
  DECLARE_LOCALS
  psdout(10) << "Active: handling ActMap" << dendl;
  ceph_assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< PeeringMachine >().get_query_map());
  }

  if (pg->cct->_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->osd->store);

  uint64_t unfound = pg->missing_loc.num_unfound();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) {
    if (pg->cct->_conf->osd_auto_mark_unfound_lost) {
      pg->osd->clog->error() << context< PeeringMachine >().spgid.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically "
			    << "mark these objects lost but this feature is not yet implemented "
			    << "(osd_auto_mark_unfound_lost)";
    } else
      pg->osd->clog->error() << context< PeeringMachine >().spgid.pgid << " has "
                             << unfound << " objects unfound and apparently lost";
  }

  if (pg->is_active()) {
    psdout(10) << "Active: kicking snap trim" << dendl;
    pg->kick_snap_trim();
  }

  if (pg->is_peered() &&
      !pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL) &&
      (!pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) || pg->is_degraded())) {
    pg->queue_recovery();
  }
  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const MNotifyRec& notevt)
{

  DECLARE_LOCALS
  ceph_assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already have info from that osd, ignoring"
		       << dendl;
  } else if (pg->peer_purged.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already purged that peer, ignoring"
		       << dendl;
  } else {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", calling proc_replica_info and discover_all_missing"
		       << dendl;
    ps->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
    if (pg->have_unfound() || (pg->is_degraded() && pg->might_have_unfound.count(notevt.from))) {
      pg->discover_all_missing(*context< PeeringMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MTrim& trim)
{
  DECLARE_LOCALS
  ceph_assert(pg->is_primary());

  // peer is informing us of their last_complete_ondisk
  ldout(pg->cct,10) << " replica osd." << trim.from << " lcod " << trim.trim_to << dendl;
  pg->peer_last_complete_ondisk[pg_shard_t(trim.from, trim.shard)] = trim.trim_to;

  // trim log when the pg is recovered
  pg->calc_min_last_complete_ondisk();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS
  ceph_assert(pg->is_primary());

  ceph_assert(!pg->acting_recovery_backfill.empty());
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_acting_recovery_backfill(infoevt.from) &&
      pg->peer_activated.count(infoevt.from) == 0) {
    psdout(10) << " peer osd." << infoevt.from
		       << " activated and committed" << dendl;
    pg->peer_activated.insert(infoevt.from);
    pg->blocked_by.erase(infoevt.from.shard);
    pg->publish_stats_to_osd();
    if (pg->peer_activated.size() == pg->acting_recovery_backfill.size()) {
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
    pg->peer_info[logevt.from],
    pg->peer_missing[logevt.from],
    logevt.from,
    context< PeeringMachine >().get_recovery_ctx());
  // If there are missing AND we are "fully" active then start recovery now
  if (got_missing && pg->state_test(PG_STATE_ACTIVE)) {
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
    for (set<pg_shard_t>::iterator p = pg->might_have_unfound.begin();
	 p != pg->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (pg->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (pg->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!pg->get_osdmap()->is_up(p->osd)) {
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

  pg->state_clear(PG_STATE_ACTIVATING);
  pg->state_clear(PG_STATE_CREATING);
  pg->state_clear(PG_STATE_PREMERGE);

  bool merge_target;
  if (pg->pool.info.is_pending_merge(pgid, &merge_target)) {
    pg->state_set(PG_STATE_PEERED);
    pg->state_set(PG_STATE_PREMERGE);

    if (pg->actingset.size() != pg->get_osdmap()->get_pg_size(pgid)) {
      if (merge_target) {
	pg_t src = pgid;
	src.set_ps(pg->pool.info.get_pg_num_pending());
	assert(src.get_parent() == pgid);
	pg->osd->set_not_ready_to_merge_target(pgid, src);
      } else {
	pg->osd->set_not_ready_to_merge_source(pgid);
      }
    }
  } else if (pg->acting.size() < pg->pool.info.min_size) {
    pg->state_set(PG_STATE_PEERED);
  } else {
    pg->state_set(PG_STATE_ACTIVE);
  }

  if (pg->pool.info.has_flag(pg_pool_t::FLAG_CREATING)) {
    pg->osd->send_pg_created(pgid);
  }

  pg->info.history.last_epoch_started = pg->info.last_epoch_started;
  pg->info.history.last_interval_started = pg->info.last_interval_started;
  pg->dirty_info = true;

  pg->share_pg_info();
  pg->publish_stats_to_osd();

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

  pg->on_activate();

  return discard_event();
}

void PeeringState::Active::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);


  DECLARE_LOCALS
  pl->cancel_local_background_io_reservation();

  pg->blocked_by.clear();
  ps->backfill_reserved = false;
  pg->backfill_reserving = false;
  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_DEGRADED);
  ps->state_clear(PG_STATE_UNDERSIZED);
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_active_latency, dur);
  pg->agent_stop();
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
  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MTrim& trim)
{
  DECLARE_LOCALS
  // primary is instructing us to trim
  pg->pg_log.trim(trim.trim_to, pg->info);
  pg->dirty_info = true;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const ActMap&)
{
  DECLARE_LOCALS
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< PeeringMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap_epoch(),
	pg->get_osdmap_epoch(),
	pg->info),
      pg->past_intervals);
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
  pg->clear_reserved_num_bytes();

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
  ceph_assert(!pg->is_peered());
  ceph_assert(!pg->is_peering());
  ceph_assert(!pg->is_primary());

  if (!pg->get_osdmap()->have_pg_pool(pg->get_pgid().pool())) {
    ldout(pg->cct,10) << __func__ << " pool is deleted" << dendl;
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
    pg->info = msg->info;
    pg->on_info_history_change();
    pg->dirty_info = true;
    pg->dirty_big_info = true;  // maybe.

    PG::PGLogEntryHandler rollbacker{pg, t};
    pg->pg_log.reset_backfill_claim_log(msg->log, &rollbacker);

    pg->pg_log.reset_backfill();
  } else {
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS
  psdout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    pg->info.stats = infoevt.info.stats;
    pg->info.hit_set = infoevt.info.hit_set;
  }

  ceph_assert(infoevt.info.last_update == pg->info.last_update);
  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

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
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< PeeringMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap_epoch(),
	pg->get_osdmap_epoch(),
	pg->info),
      pg->past_intervals);
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
  pg->deleting = true;
  ObjectStore::Transaction* t = context<PeeringMachine>().get_cur_transaction();
  pg->on_removal(t);
  t->register_on_commit(new PG::C_DeleteMore(pg, pg->get_osdmap_epoch()));
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

  pg->blocked_by.clear();
  for (set<pg_shard_t>::const_iterator it = prior_set.probe.begin();
       it != prior_set.probe.end();
       ++it) {
    pg_shard_t peer = *it;
    if (peer == pg->pg_whoami) {
      continue;
    }
    if (pg->peer_info.count(peer)) {
      psdout(10) << " have osd." << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      psdout(10) << " already requested info from osd." << peer << dendl;
      pg->blocked_by.insert(peer.osd);
    } else if (!pg->get_osdmap()->is_up(peer.osd)) {
      psdout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      psdout(10) << " querying info from osd." << peer << dendl;
      context< PeeringMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 it->shard, pg->pg_whoami.shard,
			 pg->info.history,
			 pg->get_osdmap_epoch()));
      peer_info_requested.insert(peer);
      pg->blocked_by.insert(peer.osd);
    }
  }

  pg->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetInfo::react(const MNotifyRec& infoevt)
{

  DECLARE_LOCALS

  set<pg_shard_t>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) {
    peer_info_requested.erase(p);
    pg->blocked_by.erase(infoevt.from.osd);
  }

  epoch_t old_start = pg->info.history.last_epoch_started;
  if (ps->proc_replica_info(
	infoevt.from, infoevt.notify.info, infoevt.notify.epoch_sent)) {
    // we got something new ...
    PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
    if (old_start < pg->info.history.last_epoch_started) {
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
    if (pg->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      pg->peer_info[*p].dump(q.f);
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
  pg->blocked_by.clear();
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
  if (!pg->choose_acting(auth_log_shard, false,
			 &context< Peering >().history_les_bound)) {
    if (!pg->want_acting.empty()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (auth_log_shard == pg->pg_whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = pg->peer_info[auth_log_shard];

  // am i broken?
  if (pg->info.last_update < best.log_tail) {
    psdout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->info.last_update;
  ceph_assert(!pg->acting_recovery_backfill.empty());
  for (set<pg_shard_t>::iterator p = pg->acting_recovery_backfill.begin();
       p != pg->acting_recovery_backfill.end();
       ++p) {
    if (*p == pg->pg_whoami) continue;
    pg_info_t& ri = pg->peer_info[*p];
    if (ri.last_update < pg->info.log_tail && ri.last_update >= best.log_tail &&
        ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  psdout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<PeeringMachine>().send_query(
    auth_log_shard,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, pg->pg_whoami.shard,
      request_log_from, pg->info.history,
      pg->get_osdmap_epoch()));

  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(auth_log_shard.osd);
  pg->publish_stats_to_osd();
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
  pg->blocked_by.clear();
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

  psdout(10) << "verifying no want_acting " << pg->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->want_acting.begin(); p != pg->want_acting.end(); ++p) {
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

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_DOWN);

  auto &prior_set = context< Peering >().prior_set;
  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pg->publish_stats_to_osd();
}

void PeeringState::Down::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS

  pg->state_clear(PG_STATE_DOWN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_down_latency, dur);

  pg->blocked_by.clear();
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

  ceph_assert(pg->is_primary());
  epoch_t old_start = pg->info.history.last_epoch_started;
  if (!pg->peer_info.count(infoevt.from) &&
      pg->get_osdmap()->has_been_up_since(infoevt.from.osd, infoevt.notify.epoch_sent)) {
    pg->update_history(infoevt.notify.info.history);
  }
  // if we got something new to make pg escape down state
  if (pg->info.history.last_epoch_started > old_start) {
      psdout(10) << " last_epoch_started moved forward, re-enter getinfo" << dendl;
    pg->state_clear(PG_STATE_DOWN);
    pg->state_set(PG_STATE_PEERING);
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

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);

  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pg->publish_stats_to_osd();
}

boost::statechart::result PeeringState::Incomplete::react(const AdvMap &advmap) {
  DECLARE_LOCALS
  int64_t poolnum = pg->info.pgid.pool();

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

  pg->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_incomplete_latency, dur);

  pg->blocked_by.clear();
}

/*------GetMissing--------*/
PeeringState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetMissing")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS
  ceph_assert(!pg->acting_recovery_backfill.empty());
  eversion_t since;
  for (set<pg_shard_t>::iterator i = pg->acting_recovery_backfill.begin();
       i != pg->acting_recovery_backfill.end();
       ++i) {
    if (*i == pg->get_primary()) continue;
    const pg_info_t& pi = pg->peer_info[*i];
    // reset this so to make sure the pg_missing_t is initialized and
    // has the correct semantics even if we don't need to get a
    // missing set from a shard. This way later additions due to
    // lost+unfound delete work properly.
    pg->peer_missing[*i].may_include_deletes = !pg->perform_deletes_during_peering();

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->pg_log.get_tail()) {
      psdout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      psdout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      psdout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    since.epoch = pi.last_epoch_started;
    ceph_assert(pi.last_update >= pg->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      psdout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< PeeringMachine >().send_query(
	*i,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, pg->pg_whoami.shard,
	  since, pg->info.history,
	  pg->get_osdmap_epoch()));
    } else {
      psdout(10) << " requesting fulllog+missing from osd." << *i
			 << " (want since " << since << " < log.tail "
			 << pi.log_tail << ")" << dendl;
      context< PeeringMachine >().send_query(
	*i, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, pg->pg_whoami.shard,
	  pg->info.history, pg->get_osdmap_epoch()));
    }
    peer_missing_requested.insert(*i);
    pg->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate(pg->get_osdmap_epoch()));
  } else {
    pg->publish_stats_to_osd();
  }
}

boost::statechart::result PeeringState::GetMissing::react(const MLogRec& logevt)
{
  DECLARE_LOCALS

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
    } else {
      psdout(10) << "Got last missing, don't need missing "
			 << "posting Activate" << dendl;
      post_event(Activate(pg->get_osdmap_epoch()));
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
    if (pg->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      pg->peer_missing[*p].dump(q.f);
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
  pg->blocked_by.clear();
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
  if (!pg->need_up_thru) {
    post_event(Activate(pg->get_osdmap_epoch()));
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const MLogRec& logevt)
{
  DECLARE_LOCALS
  psdout(10) << "Noting missing from osd." << logevt.from << dendl;
  pg->peer_missing[logevt.from].claim(logevt.msg->missing);
  pg->peer_info[logevt.from] = logevt.msg->info;
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
