// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_shard_manager.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> PGShardManager::start(
  const int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc,
  crimson::os::FuturizedStore &store)
{
  ceph_assert(seastar::this_shard_id() == 0);
  return osd_singleton_state.start_single(
    whoami, std::ref(cluster_msgr), std::ref(public_msgr),
    std::ref(monc), std::ref(mgrc)
  ).then([this, whoami, &store] {
    // TODO
    return shard_services.start(
      std::ref(get_osd_singleton_state()), whoami, std::ref(store));
  });
}

seastar::future<> PGShardManager::stop() {
  ceph_assert(seastar::this_shard_id() == 0);
  return shard_services.stop(
  ).then([this] {
    return osd_singleton_state.stop();
  });
}

seastar::future<> PGShardManager::load_pgs()
{
  return get_local_state().store.list_collections(
  ).then([this](auto colls) {
    return seastar::parallel_for_each(
      colls,
      [this](auto coll) {
	spg_t pgid;
	if (coll.is_pg(&pgid)) {
	  auto core = get_osd_singleton_state(
	  ).pg_to_shard_mapping.maybe_create_pg(
	    pgid);

	  return with_remote_shard_state(
	    core,
	    [pgid](
	      PerShardState &per_shard_state,
	      ShardServices &shard_services) {
	      return shard_services.load_pg(
		pgid
	      ).then([pgid, &per_shard_state, &shard_services](auto &&pg) {
		logger().info("load_pgs: loaded {}", pgid);
		per_shard_state.pg_map.pg_loaded(pgid, std::move(pg));
		shard_services.inc_pg_num();
		return seastar::now();
	      });
	    });
	} else if (coll.is_temp(&pgid)) {
	  logger().warn(
	    "found temp collection on crimson osd, should be impossible: {}",
	    coll);
	  ceph_assert(0 == "temp collection on crimson osd, should be impossible");
	  return seastar::now();
	} else {
	  logger().warn("ignoring unrecognized collection: {}", coll);
	  return seastar::now();
	}
      });
  });
}

seastar::future<> PGShardManager::stop_pgs()
{
  return get_local_state().stop_pgs();
}

seastar::future<std::map<pg_t, pg_stat_t>>
PGShardManager::get_pg_stats() const
{
  return seastar::make_ready_future<std::map<pg_t, pg_stat_t>>(
    get_local_state().get_pg_stats());
}

seastar::future<> PGShardManager::broadcast_map_to_pgs(epoch_t epoch)
{
  return get_local_state().broadcast_map_to_pgs(
    get_shard_services(), epoch
  ).then([this, epoch] {
    get_osd_singleton_state().osdmap_gate.got_map(epoch);
    return seastar::now();
  });
}

}
