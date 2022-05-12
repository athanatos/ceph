// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>

#include "crimson/osd/shard_services.h"

namespace crimson::osd {

/**
 * PGShardManager
 *
 * Manages all state required to partition PGs over seastar reactors
 * as well as state required to route messages to pgs and all shared
 * resources required by PGs (objectstore, messenger, monclient, etc)
 */
class PGShardManager {
  ShardServices shard_services;
public:
  template <typename... Args>
  PGShardManager(Args&&... args) : shard_services(std::forward<Args>(args)...) {}
    
  auto &get_shard_services() { return shard_services; }
};

}
