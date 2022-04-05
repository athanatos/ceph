// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <functional>
#include <map>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/osd/osd_operation.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSDMapGate {
protected:
  struct OSDMapBlocker : public BlockerT<OSDMapBlocker> {
    const char * type_name;
    epoch_t epoch;

    OSDMapBlocker(std::pair<const char *, epoch_t> args)
      : type_name(args.first), epoch(args.second) {}

    OSDMapBlocker(const OSDMapBlocker &) = delete;
    OSDMapBlocker(OSDMapBlocker &&) = delete;
    OSDMapBlocker &operator=(const OSDMapBlocker &) = delete;
    OSDMapBlocker &operator=(OSDMapBlocker &&) = delete;

    seastar::shared_promise<epoch_t> promise;

    void dump_detail(Formatter *f) const final;
  };

  // order the promises in ascending order of the waited osdmap epoch,
  // so we can access all the waiters expecting a map whose epoch is less
  // than or equal to a given epoch
  using waiting_peering_t = std::map<epoch_t,
				     OSDMapBlocker>;
  const char *blocker_type;
  waiting_peering_t waiting_peering;
  epoch_t current = 0;
  std::optional<std::reference_wrapper<ShardServices>> shard_services;
  bool stopping = false;
public:
  OSDMapGate(
    const char *blocker_type,
    std::optional<std::reference_wrapper<ShardServices>> shard_services)
    : blocker_type(blocker_type), shard_services(shard_services) {}

  // wait for an osdmap whose epoch is greater or equal to given epoch
  blocking_future<epoch_t>
  wait_for_map(epoch_t epoch);
  // TODO: define me!
  seastar::future<epoch_t>
  wait_for_map(OSDMapBlocker::BlockingEvent::TriggerI&& trigger,
	       epoch_t epoch);
  void got_map(epoch_t epoch);
  seastar::future<> stop();
};


enum class OSDMapGateType {
  OSD,
  PG,
  last_item
};

template <OSDMapGateType GateTypeV>
struct ConcreteOSDMapGate : OSDMapGate  {
  struct ConcreteOSDMapBlocker : OSDMapBlocker {
    using OSDMapBlocker::OSDMapBlocker;
  };

  using OSDMapGate::OSDMapGate;
  using OSDMapGate::got_map;
  using OSDMapGate::stop;

  using BlockingEvent = typename ConcreteOSDMapBlocker::BlockingEvent;

  // just for type safety. We want to ensure the appropriate filler is used.
  seastar::future<epoch_t>
  wait_for_map(typename ConcreteOSDMapBlocker::BlockingEvent::TriggerI&& trigger,
	       epoch_t epoch) {
    return OSDMapGate::wait_for_map(std::move(trigger), epoch);

  }
};

using OSD_OSDMapGate = ConcreteOSDMapGate<OSDMapGateType::OSD>;
using PG_OSDMapGate = ConcreteOSDMapGate<OSDMapGateType::PG>;

}
