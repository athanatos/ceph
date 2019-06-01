// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/osd/osd_operation.h"

namespace ceph {
  class Formatter;
  namespace osd {
    class ShardServices;
  }
}

namespace ceph::osd {

class OSDMapGate {
  struct OSDMapBlocker : public BlockerT<OSDMapBlocker> {
    constexpr static const char * type_name = "OSDMapBlocker";
    epoch_t epoch;

    OSDMapBlocker(epoch_t epoch) : epoch(epoch) {}

    seastar::shared_promise<epoch_t> promise;
    seastar::future<epoch_t> block_op(OperationRef op);
    virtual void dump_detail(Formatter *f) const override final;
  };
  
  // order the promises in descending order of the waited osdmap epoch,
  // so we can access all the waiters expecting a map whose epoch is less
  // than a given epoch
  using waiting_peering_t = std::map<epoch_t,
				     OSDMapBlocker,
				     std::greater<epoch_t>>;
  waiting_peering_t waiting_peering;
  epoch_t current = 0;
  ShardServices &shard_services;
public:
  OSDMapGate(ShardServices &shard_services) : shard_services(shard_services) {}

  // wait for an osdmap whose epoch is greater or equal to given epoch
  seastar::future<epoch_t> wait_for_map(OperationRef req, epoch_t epoch);
  void got_map(epoch_t epoch);
};

}
