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

PGShardManager::PGShardManager(
  const int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc,
  crimson::os::FuturizedStore &store)
  : osd_singleton_state(whoami, cluster_msgr, public_msgr,
			monc, mgrc, store),
    local_state(whoami),
    shard_services(osd_singleton_state, local_state)
{}

seastar::future<Ref<PG>> PGShardManager::make_pg(
  ShardServices &shard_services,
  OSDMapService::cached_map_t create_map,
  spg_t pgid,
  bool do_create)
{
  return seastar::make_ready_future<Ref<PG>>();
#if 0
  using ec_profile_t = std::map<std::string, std::string>;
  auto get_pool_info = [create_map, pgid, this] {
    if (create_map->have_pg_pool(pgid.pool())) {
      pg_pool_t pi = *create_map->get_pg_pool(pgid.pool());
      std::string name = create_map->get_pool_name(pgid.pool());
      ec_profile_t ec_profile;
      if (pi.is_erasure()) {
	ec_profile = create_map->get_erasure_code_profile(
	  pi.erasure_code_profile);
      }
      return seastar::make_ready_future<
	std::tuple<pg_pool_t,std::string, ec_profile_t>
	>(std::make_tuple(
	    std::move(pi),
	    std::move(name),
	    std::move(ec_profile)));
    } else {
      // pool was deleted; grab final pg_pool_t off disk.
      return get_meta_coll().load_final_pool_info(pgid.pool());
    }
  };
  auto get_collection = [pgid, do_create, this] {
    const coll_t cid{pgid};
    if (do_create) {
      return store.create_new_collection(cid);
    } else {
      return store.open_collection(cid);
    }
  };
  return seastar::when_all(
    std::move(get_pool_info),
    std::move(get_collection)
  ).then([&shard_services, pgid, create_map, this] (auto&& ret) {
    auto [pool, name, ec_profile] = std::move(std::get<0>(ret).get0());
    auto coll = std::move(std::get<1>(ret).get0());
    return seastar::make_ready_future<Ref<PG>>(
      new PG{
	pgid,
	pg_shard_t{whoami, pgid.shard},
	std::move(coll),
	std::move(pool),
	std::move(name),
	create_map,
	shard_services,
	ec_profile});
  });
#endif
}

seastar::future<Ref<PG>> PGShardManager::handle_pg_create_info(
  ShardServices &shard_services,
  std::unique_ptr<PGCreateInfo> info) {
  return seastar::make_ready_future<Ref<PG>>();
#if 0
  return seastar::do_with(
    std::move(info),
    [this, &shard_services](auto &info)
    -> seastar::future<Ref<PG>> {
      return get_map(info->epoch).then(
	[&info, &shard_services, this](OSDMapService::cached_map_t startmap)
	-> seastar::future<std::tuple<Ref<PG>, OSDMapService::cached_map_t>> {
	  const spg_t &pgid = info->pgid;
	  if (info->by_mon) {
	    int64_t pool_id = pgid.pgid.pool();
	    const pg_pool_t *pool = get_osdmap()->get_pg_pool(pool_id);
	    if (!pool) {
	      logger().debug(
		"{} ignoring pgid {}, pool dne",
		__func__,
		pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	    ceph_assert(osdmap->require_osd_release >= ceph_release_t::octopus);
	    if (!pool->has_flag(pg_pool_t::FLAG_CREATING)) {
	      // this ensures we do not process old creating messages after the
	      // pool's initial pgs have been created (and pg are subsequently
	      // allowed to split or merge).
	      logger().debug(
		"{} dropping {} create, pool does not have CREATING flag set",
		__func__,
		pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	  }
	  return make_pg(shard_services, startmap, pgid, true).then(
	    [startmap=std::move(startmap)](auto pg) mutable {
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(std::move(pg), std::move(startmap)));
	    });
	}).then([this, &shard_services, &info](auto&& ret)
		->seastar::future<Ref<PG>> {
	  auto [pg, startmap] = std::move(ret);
	  if (!pg)
	    return seastar::make_ready_future<Ref<PG>>(Ref<PG>());
	  const pg_pool_t* pp = startmap->get_pg_pool(info->pgid.pool());

	  int up_primary, acting_primary;
	  vector<int> up, acting;
	  startmap->pg_to_up_acting_osds(
	    info->pgid.pgid, &up, &up_primary, &acting, &acting_primary);

	  int role = startmap->calc_pg_role(
	    pg_shard_t(whoami, info->pgid.shard),
	    acting);

	  PeeringCtx rctx;
	  create_pg_collection(
	    rctx.transaction,
	    info->pgid,
	    info->pgid.get_split_bits(pp->get_pg_num()));
	  init_pg_ondisk(
	    rctx.transaction,
	    info->pgid,
	    pp);

	  pg->init(
	    role,
	    up,
	    up_primary,
	    acting,
	    acting_primary,
	    info->history,
	    info->past_intervals,
	    rctx.transaction);

	  return shard_services.start_operation<PGAdvanceMap>(
	    shard_services, pg, pg->get_osdmap_epoch(),
	    osdmap->get_epoch(), std::move(rctx), true).second.then([pg=pg] {
	      return seastar::make_ready_future<Ref<PG>>(pg);
	    });
	});
    });
#endif
}


seastar::future<Ref<PG>>
PGShardManager::get_or_create_pg(
  ShardServices &shard_services,
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger,
  spg_t pgid,
  epoch_t epoch,
  std::unique_ptr<PGCreateInfo> info)
{
  return seastar::make_ready_future<Ref<PG>>();
#if 0
  if (info) {
    auto [fut, creating] = pg_map.wait_for_pg(std::move(trigger), pgid);
    if (!creating) {
      pg_map.set_creating(pgid);
      (void)handle_pg_create_info(
	shard_services, std::move(info));
    }
    return std::move(fut);
  } else {
    return seastar::make_ready_future<Ref<PG>>(pg_map.get_pg(pgid));
  }
#endif
}

seastar::future<Ref<PG>> PGShardManager::wait_for_pg(
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger, spg_t pgid)
{
  return local_state.pg_map.wait_for_pg(std::move(trigger), pgid).first;
}

Ref<PG> PGShardManager::get_pg(spg_t pgid)
{
  return local_state.pg_map.get_pg(pgid);
}

seastar::future<> PGShardManager::load_pgs()
{
  return seastar::now();
#if 0
  return store.list_collections(
  ).then([this, &shard_services](auto colls) {
    return seastar::parallel_for_each(
      colls,
      [this, &shard_services](auto coll) {
	spg_t pgid;
	if (coll.is_pg(&pgid)) {
	  return load_pg(
	    shard_services,
	    pgid
	  ).then([pgid, this, &shard_services](auto &&pg) {
	    logger().info("load_pgs: loaded {}", pgid);
	    pg_map.pg_loaded(pgid, std::move(pg));
	    shard_services.inc_pg_num();
	    return seastar::now();
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
#endif
}

seastar::future<Ref<PG>> PGShardManager::load_pg(
  ShardServices &shard_services,
  spg_t pgid)
{
  logger().debug("{}: {}", __func__, pgid);
  return seastar::make_ready_future<Ref<PG>>();
#if 0

  return seastar::do_with(PGMeta(store, pgid), [](auto& pg_meta) {
    return pg_meta.get_epoch();
  }).then([this](epoch_t e) {
    return get_map(e);
  }).then([pgid, this, &shard_services] (auto&& create_map) {
    return make_pg(shard_services, std::move(create_map), pgid, false);
  }).then([this](Ref<PG> pg) {
    return pg->read_state(&store).then([pg] {
	return seastar::make_ready_future<Ref<PG>>(std::move(pg));
    });
  }).handle_exception([pgid](auto ep) {
    logger().info("pg {} saw exception on load {}", pgid, ep);
    ceph_abort("Could not load pg" == 0);
    return seastar::make_exception_future<Ref<PG>>(ep);
  });
#endif
}


seastar::future<> PGShardManager::stop_pgs()
{
  return local_state.stop_pgs();
}

std::map<pg_t, pg_stat_t> PGShardManager::get_pg_stats() const
{
  return local_state.get_pg_stats();
}

seastar::future<> PGShardManager::broadcast_map_to_pgs(epoch_t epoch)
{
  return local_state.broadcast_map_to_pgs(
    shard_services, epoch);
}

}
