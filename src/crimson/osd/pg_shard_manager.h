// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>

#include "crimson/osd/shard_services.h"
#include "crimson/osd/pg_map.h"

namespace crimson::osd {

/**
 * PGShardManager
 *
 * Manages all state required to partition PGs over seastar reactors
 * as well as state required to route messages to pgs. Mediates access to
 * shared resources required by PGs (objectstore, messenger, monclient,
 * etc)
 */
class PGShardManager {
  OSDSingletonState osd_singleton_state;
  PerShardState local_state;
  ShardServices shard_services;

public:
  PGShardManager(
    const int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);

  auto &get_shard_services() { return shard_services; }

  void update_map(OSDMapService::cached_map_t map) {
    osd_singleton_state.update_map(map);
    local_state.update_map(map);
  }

  auto stop_registries() {
    return local_state.stop_registry();
  }

  FORWARD_TO_OSD_SINGLETON(send_pg_created)

  // osd state forwards
  FORWARD(is_active, is_active, osd_singleton_state.osd_state)
  FORWARD(is_preboot, is_preboot, osd_singleton_state.osd_state)
  FORWARD(is_booting, is_booting, osd_singleton_state.osd_state)
  FORWARD(is_stopping, is_stopping, osd_singleton_state.osd_state)
  FORWARD(is_prestop, is_prestop, osd_singleton_state.osd_state)
  FORWARD(is_initializing, is_initializing, osd_singleton_state.osd_state)
  FORWARD(set_prestop, set_prestop, osd_singleton_state.osd_state)
  FORWARD(set_preboot, set_preboot, osd_singleton_state.osd_state)
  FORWARD(set_booting, set_booting, osd_singleton_state.osd_state)
  FORWARD(set_stopping, set_stopping, osd_singleton_state.osd_state)
  FORWARD(set_active, set_active, osd_singleton_state.osd_state)
  FORWARD(when_active, when_active, osd_singleton_state.osd_state)
  FORWARD_CONST(get_osd_state_string, to_string, osd_singleton_state.osd_state)

  FORWARD(got_map, got_map, osd_singleton_state.osdmap_gate)
  FORWARD(wait_for_map, wait_for_map, osd_singleton_state.osdmap_gate)

  // Metacoll
  FORWARD_TO_OSD_SINGLETON(init_meta_coll)
  FORWARD_TO_OSD_SINGLETON(get_meta_coll)

  // Core OSDMap methods
  FORWARD_TO_OSD_SINGLETON(get_map)
  FORWARD_TO_OSD_SINGLETON(load_map_bl)
  FORWARD_TO_OSD_SINGLETON(load_map_bls)
  FORWARD_TO_OSD_SINGLETON(store_maps)
  FORWARD_TO_OSD_SINGLETON(get_up_epoch)
  FORWARD_TO_OSD_SINGLETON(set_up_epoch)

  using cached_map_t = OSDMapService::cached_map_t;

  /// Runs opref on the appropriate core, creating the pg as necessary.  
  template <typename T>
  seastar::future<> run_with_pg_maybe_create(
    T::Ref opref
  ) {
    static_assert(T::can_create());
    logger.debug("{}: can_create", opref);
    // TODOSAM: release opref from local registry and relink on the other side
    // TODOSAM: move with_blocking_event here into remote core
    
    auto core = osd_singleton_state.pg_to_shard_mapping.maybe_create_pg(
      opref.get_pgid());

    return smp::submit_to(
      core,
      [opref=std::move(opref)] {
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    return run_with_pg_maybe_create(
	      shard_services,
	      std::move(trigger),
	      opref.get_pgid(), opref.get_epoch(),
	      std::move(opref.get_create_info()));
	  });
      });
  }

  /// Runs opref on the appropriate core, waiting for pg as necessary
  template <typename T>
  seastar::future<> run_with_pg(
    spg_t pgid,             ///< [in] pgid of pg targert
    typename T::Ref &&op    ///< [in] op to run
  ) {
    return seastar::now();
  }

  seastar::future<Ref<PG>> make_pg(
    ShardServices &shard_services,
    cached_map_t create_map,
    spg_t pgid,
    bool do_create);
  seastar::future<Ref<PG>> handle_pg_create_info(
    ShardServices &shard_services,
    std::unique_ptr<PGCreateInfo> info);
  seastar::future<Ref<PG>> get_or_create_pg(
    ShardServices &shard_services,
    PGMap::PGCreationBlockingEvent::TriggerI&&,
    spg_t pgid,
    epoch_t epoch,
    std::unique_ptr<PGCreateInfo> info);
  seastar::future<Ref<PG>> wait_for_pg(
    PGMap::PGCreationBlockingEvent::TriggerI&&, spg_t pgid);
  Ref<PG> get_pg(spg_t pgid);
  seastar::future<Ref<PG>> load_pg(
    ShardServices &shard_services,
    spg_t pgid);
  seastar::future<> load_pgs();
  seastar::future<> stop_pgs();

  // TODOSAM: needs to be future
  std::map<pg_t, pg_stat_t> get_pg_stats() const;

  template <typename F>
  void for_each_pg(F &&f) const {
    // TODOSAM -- needs to be a future, map over all cores
  }
  auto get_num_pgs() const {
    // TODOSAM -- needs to be a future, map over all cores
    return local_state.pg_map.get_pgs().size();
  }

  seastar::future<> broadcast_map_to_pgs(epoch_t epoch);

  template <typename F>
  auto with_pg(spg_t pgid, F &&f) {
    return std::invoke(
      std::forward<F>(f),
      local_state.pg_map.get_pg(pgid));
  }

  template <typename T, typename... Args>
  auto start_pg_operation(Args&&... args) {
    auto op = local_state.registry.create_operation<T>(
      std::forward<Args>(args)...);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    logger.debug("{}: starting {}", *op, __func__);
    auto &opref = *op;

    auto fut = opref.template enter_stage<>(
      opref.get_connection_pipeline().await_active
    ).then([this, &opref, &logger] {
      logger.debug("{}: start_pg_operation in await_active stage", opref);
      return osd_singleton_state.osd_state.when_active();
    }).then([&logger, &opref] {
      logger.debug("{}: start_pg_operation active, entering await_map", opref);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().await_map);
    }).then([this, &logger, &opref] {
      logger.debug("{}: start_pg_operation await_map stage", opref);
      using OSDMapBlockingEvent =
	OSD_OSDMapGate::OSDMapBlocker::BlockingEvent;
      return opref.template with_blocking_event<OSDMapBlockingEvent>(
	[this, &opref](auto &&trigger) {
	  std::ignore = this;
	  return osd_singleton_state.osdmap_gate.wait_for_map(
	    std::move(trigger),
	    opref.get_epoch(),
	    &shard_services);
	});
    }).then([&logger, &opref](auto epoch) {
      logger.debug("{}: got map {}, entering get_pg", opref, epoch);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().get_pg);
    }).then([this, &logger, &opref] {
      logger.debug("{}: in get_pg", opref);
      if constexpr (T::can_create()) {
	logger.debug("{}: can_create", opref);
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    return run_with_pg_maybe_create(
	      shard_services,
	      std::move(trigger),
	      opref.get_pgid(), opref.get_epoch(),
	      std::move(opref.get_create_info()));
	  });
      } else {
	logger.debug("{}: !can_create", opref);
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    return wait_for_pg(
	      std::move(trigger), opref.get_pgid());
	  });
      }
    }).then([this, &logger, &opref](Ref<PG> pgref) {
      logger.debug("{}: have_pg", opref);
      return opref.with_pg(get_shard_services(), pgref);
    }).then([op] { /* Retain refcount on op until completion */ });

    return std::make_pair(std::move(op), std::move(fut));
  }
};

}
