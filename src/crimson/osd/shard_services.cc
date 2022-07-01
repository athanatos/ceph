// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/smart_ptr/make_local_shared.hpp>

#include "crimson/osd/shard_services.h"

#include "messages/MOSDAlive.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"

#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/os/cyanstore/cyan_store.h"
#include "crimson/osd/osdmap_service.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using std::vector;

namespace crimson::osd {

PerShardState::PerShardState(
  int whoami)
  : whoami(whoami),
    throttler(crimson::common::local_conf()),
    obc_registry(crimson::common::local_conf())
{
  perf = build_osd_logger(&cct);
  cct.get_perfcounters_collection()->add(perf);

  recoverystate_perf = build_recoverystate_perf(&cct);
  cct.get_perfcounters_collection()->add(recoverystate_perf);
}

CoreState::CoreState(
  int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc,
  crimson::os::FuturizedStore &store)
  : whoami(whoami),
    osdmap_gate("CoreState::osdmap_gate"),
    cluster_msgr(cluster_msgr),
    public_msgr(public_msgr),
    monc(monc),
    mgrc(mgrc),
    store(store),
    local_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority),
    remote_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority)
{
  crimson::common::local_conf().add_observer(this);
  osdmaps[0] = boost::make_local_shared<OSDMap>();
}

seastar::future<> CoreState::send_to_osd(
  int peer, MessageURef m, epoch_t from_epoch)
{
  if (osdmap->is_down(peer)) {
    logger().info("{}: osd.{} is_down", __func__, peer);
    return seastar::now();
  } else if (osdmap->get_info(peer).up_from > from_epoch) {
    logger().info("{}: osd.{} {} > {}", __func__, peer,
		    osdmap->get_info(peer).up_from, from_epoch);
    return seastar::now();
  } else {
    auto conn = cluster_msgr.connect(
        osdmap->get_cluster_addrs(peer).front(), CEPH_ENTITY_TYPE_OSD);
    return conn->send(std::move(m));
  }
}

seastar::future<> CoreState::osdmap_subscribe(version_t epoch, bool force_request)
{
  logger().info("{}({})", __func__, epoch);
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

void CoreState::queue_want_pg_temp(pg_t pgid,
				       const vector<int>& want,
				       bool forced)
{
  auto p = pg_temp_pending.find(pgid);
  if (p == pg_temp_pending.end() ||
      p->second.acting != want ||
      forced) {
    pg_temp_wanted[pgid] = {want, forced};
  }
}

void CoreState::remove_want_pg_temp(pg_t pgid)
{
  pg_temp_wanted.erase(pgid);
  pg_temp_pending.erase(pgid);
}

void CoreState::requeue_pg_temp()
{
  unsigned old_wanted = pg_temp_wanted.size();
  unsigned old_pending = pg_temp_pending.size();
  pg_temp_wanted.merge(pg_temp_pending);
  pg_temp_pending.clear();
  logger().debug(
    "{}: {} + {} -> {}",
    __func__ ,
    old_wanted,
    old_pending,
    pg_temp_wanted.size());
}

seastar::future<> CoreState::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return seastar::now();
  logger().debug("{}: {}", __func__, pg_temp_wanted);
  MURef<MOSDPGTemp> ms[2] = {nullptr, nullptr};
  for (auto& [pgid, pg_temp] : pg_temp_wanted) {
    auto& m = ms[pg_temp.forced];
    if (!m) {
      m = crimson::make_message<MOSDPGTemp>(osdmap->get_epoch());
      m->forced = pg_temp.forced;
    }
    m->pg_temp.emplace(pgid, pg_temp.acting);
  }
  pg_temp_pending.merge(pg_temp_wanted);
  pg_temp_wanted.clear();
  return seastar::parallel_for_each(std::begin(ms), std::end(ms),
    [this](auto& m) {
      if (m) {
	return monc.send_message(std::move(m));
      } else {
	return seastar::now();
      }
    });
}

std::ostream& operator<<(
  std::ostream& out,
  const CoreState::pg_temp_t& pg_temp)
{
  out << pg_temp.acting;
  if (pg_temp.forced) {
    out << " (forced)";
  }
  return out;
}

seastar::future<> CoreState::send_pg_created(pg_t pgid)
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  pg_created.insert(pgid);
  return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
}

seastar::future<> CoreState::send_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  return seastar::parallel_for_each(pg_created,
    [this](auto &pgid) {
      return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
    });
}

void CoreState::prune_pg_created()
{
  logger().debug(__func__);
  auto o = get_osdmap();
  auto i = pg_created.begin();
  while (i != pg_created.end()) {
    auto p = o->get_pg_pool(i->pool());
    if (!p || !p->has_flag(pg_pool_t::FLAG_CREATING)) {
      logger().debug("{} pruning {}", __func__, *i);
      i = pg_created.erase(i);
    } else {
      logger().debug(" keeping {}", __func__, *i);
      ++i;
    }
  }
}

HeartbeatStampsRef CoreState::get_hb_stamps(int peer)
{
  auto [stamps, added] = heartbeat_stamps.try_emplace(peer);
  if (added) {
    stamps->second = ceph::make_ref<HeartbeatStamps>(peer);
  }
  return stamps->second;
}

seastar::future<> CoreState::send_alive(const epoch_t want)
{
  logger().info(
    "{} want={} up_thru_wanted={}",
    __func__,
    want,
    up_thru_wanted);

  if (want > up_thru_wanted) {
    up_thru_wanted = want;
  } else {
    logger().debug("{} want={} <= up_thru_wanted={}; skipping",
                   __func__, want, up_thru_wanted);
    return seastar::now();
  }
  if (!osdmap->exists(whoami)) {
    logger().warn("{} DNE", __func__);
    return seastar::now();
  } if (const epoch_t up_thru = osdmap->get_up_thru(whoami);
        up_thru_wanted > up_thru) {
    logger().debug("{} up_thru_wanted={} up_thru={}", __func__, want, up_thru);
    return monc.send_message(
      crimson::make_message<MOSDAlive>(osdmap->get_epoch(), want));
  } else {
    logger().debug("{} {} <= {}", __func__, want, osdmap->get_up_thru(whoami));
    return seastar::now();
  }
}

const char** CoreState::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_max_backfills",
    "osd_min_recovery_priority",
    nullptr
  };
  return KEYS;
}

void CoreState::handle_conf_change(const ConfigProxy& conf,
				   const std::set <std::string> &changed)
{
  if (changed.count("osd_max_backfills")) {
    local_reserver.set_max(conf->osd_max_backfills);
    remote_reserver.set_max(conf->osd_max_backfills);
  }
  if (changed.count("osd_min_recovery_priority")) {
    local_reserver.set_min_priority(conf->osd_min_recovery_priority);
    remote_reserver.set_min_priority(conf->osd_min_recovery_priority);
  }
}

CoreState::cached_map_t CoreState::get_map() const
{
  return osdmap;
}

seastar::future<CoreState::cached_map_t> CoreState::get_map(epoch_t e)
{
  // TODO: use LRU cache for managing osdmap, fallback to disk if we have to
  if (auto found = osdmaps.find(e); found) {
    return seastar::make_ready_future<cached_map_t>(std::move(found));
  } else {
    return load_map(e).then([e, this](std::unique_ptr<OSDMap> osdmap) {
      return seastar::make_ready_future<cached_map_t>(
        osdmaps.insert(e, std::move(osdmap)));
    });
  }
}

void CoreState::store_map_bl(ceph::os::Transaction& t,
                       epoch_t e, bufferlist&& bl)
{
  meta_coll->store_map(t, e, bl);
  map_bl_cache.insert(e, std::move(bl));
}

seastar::future<bufferlist> CoreState::load_map_bl(epoch_t e)
{
  if (std::optional<bufferlist> found = map_bl_cache.find(e); found) {
    return seastar::make_ready_future<bufferlist>(*found);
  } else {
    return meta_coll->load_map(e);
  }
}

seastar::future<std::map<epoch_t, bufferlist>> CoreState::load_map_bls(
  epoch_t first,
  epoch_t last)
{
  return seastar::map_reduce(boost::make_counting_iterator<epoch_t>(first),
			     boost::make_counting_iterator<epoch_t>(last + 1),
			     [this](epoch_t e) {
    return load_map_bl(e).then([e](auto&& bl) {
      return seastar::make_ready_future<std::pair<epoch_t, bufferlist>>(
	std::make_pair(e, std::move(bl)));
    });
  },
  std::map<epoch_t, bufferlist>{},
  [](auto&& bls, auto&& epoch_bl) {
    bls.emplace(std::move(epoch_bl));
    return std::move(bls);
  });
}

seastar::future<std::unique_ptr<OSDMap>> CoreState::load_map(epoch_t e)
{
  auto o = std::make_unique<OSDMap>();
  if (e > 0) {
    return load_map_bl(e).then([o=std::move(o)](bufferlist bl) mutable {
      o->decode(bl);
      return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
    });
  } else {
    return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
  }
}

seastar::future<> CoreState::store_maps(ceph::os::Transaction& t,
                                  epoch_t start, Ref<MOSDMap> m)
{
  return seastar::do_for_each(boost::make_counting_iterator(start),
                              boost::make_counting_iterator(m->get_last() + 1),
                              [&t, m, this](epoch_t e) {
    if (auto p = m->maps.find(e); p != m->maps.end()) {
      auto o = std::make_unique<OSDMap>();
      o->decode(p->second);
      logger().info("store_maps osdmap.{}", e);
      store_map_bl(t, e, std::move(std::move(p->second)));
      osdmaps.insert(e, std::move(o));
      return seastar::now();
    } else if (auto p = m->incremental_maps.find(e);
               p != m->incremental_maps.end()) {
      return load_map(e - 1).then([e, bl=p->second, &t, this](auto o) {
        OSDMap::Incremental inc;
        auto i = bl.cbegin();
        inc.decode(i);
        o->apply_incremental(inc);
        bufferlist fbl;
        o->encode(fbl, inc.encode_features | CEPH_FEATURE_RESERVED);
        store_map_bl(t, e, std::move(fbl));
        osdmaps.insert(e, std::move(o));
        return seastar::now();
      });
    } else {
      logger().error("MOSDMap lied about what maps it had?");
      return seastar::now();
    }
  });
}

seastar::future<> ShardServices::dispatch_context_transaction(
  crimson::os::CollectionRef col, PeeringCtx &ctx) {
  if (ctx.transaction.empty()) {
    logger().debug("ShardServices::dispatch_context_transaction: empty transaction");
    return seastar::now();
  }

  logger().debug("ShardServices::dispatch_context_transaction: do_transaction ...");
  auto ret = get_store().do_transaction(
    col,
    std::move(ctx.transaction));
  ctx.reset_transaction();
  return ret;
}

seastar::future<> ShardServices::dispatch_context_messages(
  BufferedRecoveryMessages &&ctx)
{
  auto ret = seastar::parallel_for_each(std::move(ctx.message_map),
    [this](auto& osd_messages) {
      auto& [peer, messages] = osd_messages;
      logger().debug("dispatch_context_messages sending messages to {}", peer);
      return seastar::parallel_for_each(
        std::move(messages), [=, peer=peer](auto& m) {
        return send_to_osd(peer, std::move(m), local_state.osdmap->get_epoch());
      });
    });
  ctx.message_map.clear();
  return ret;
}

seastar::future<> ShardServices::dispatch_context(
  crimson::os::CollectionRef col,
  PeeringCtx &&ctx)
{
  ceph_assert(col || ctx.transaction.empty());
  return seastar::when_all_succeed(
    dispatch_context_messages(
      BufferedRecoveryMessages{ctx}),
    col ? dispatch_context_transaction(col, ctx) : seastar::now()
  ).then_unpack([] {
    return seastar::now();
  });
}


};
