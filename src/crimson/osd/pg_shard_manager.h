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
 * as well as state required to route messages to pgs and all shared
 * resources required by PGs (objectstore, messenger, monclient, etc)
 */
class PGShardManager {
  CoreState core_state;
  PerShardState local_state;
  ShardServices shard_services;

public:
  PGShardManager(
    OSDMapService &osdmap_service,
    const int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);

  auto &get_shard_services() { return shard_services; }

  void update_map(OSDMapService::cached_map_t map) {
    core_state.update_map(map);
    local_state.update_map(map);
  }

  FORWARD_TO_CORE(send_pg_created)

  FORWARD_TO_LOCAL(update_map);

  // osd state forwards
  FORWARD(is_active, is_active, local_state.osd_state);
  FORWARD(is_preboot, is_preboot, local_state.osd_state);
  FORWARD(is_booting, is_booting, local_state.osd_state);
  FORWARD(is_stopping, is_stopping, local_state.osd_state);
  FORWARD(is_prestop, is_prestop, local_state.osd_state);
  FORWARD(is_initializing, is_initializing, local_state.osd_state);
  FORWARD(set_prestop, set_prestop, local_state.osd_state);
  FORWARD(set_preboot, set_preboot, local_state.osd_state);
  FORWARD(set_booting, set_booting, local_state.osd_state);
  FORWARD(set_stopping, set_stopping, local_state.osd_state);
  FORWARD(set_active, set_active, local_state.osd_state);
  FORWARD_CONST(get_osd_state_string, to_string, local_state.osd_state);

  FORWARD(got_map, got_map, core_state.osdmap_gate);

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
      return local_state.osd_state.when_active();
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
	  return core_state.osdmap_gate.wait_for_map(
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
	    std::ignore = opref; // TODO (next commit)
/*
	    return get_or_create_pg(
	      std::move(trigger),
	      opref.get_pgid(), opref.get_epoch(),
	      std::move(opref.get_create_info()));
	      */
	    return seastar::make_ready_future<Ref<PG>>();
	  });
      } else {
	logger.debug("{}: !can_create", opref);
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    std::ignore = opref; // TODO (next commit)
	    //return wait_for_pg(std::move(trigger), opref.get_pgid());
	    return seastar::make_ready_future<Ref<PG>>();
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
