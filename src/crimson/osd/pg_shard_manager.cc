// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_shard_manager.h"

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

seastar::future<> PGShardManager::load_pgs()
{
  return local_state.load_pgs(shard_services);
}

seastar::future<> PGShardManager::stop_pgs()
{
  return local_state.stop_pgs();
}

seastar::future<std::map<pg_t, pg_stat_t>> PGShardManager::get_pg_stats() const
{
  return seastar::make_ready_future<std::map<pg_t, pg_stat_t>>(
    local_state.get_pg_stats());
}

seastar::future<> PGShardManager::broadcast_map_to_pgs(epoch_t epoch)
{
  return local_state.broadcast_map_to_pgs(
    *this, shard_services, epoch);
}

}
