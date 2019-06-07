// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

void OSDMapGate::OSDMapBlocker::dump_detail(Formatter *f) const
{
  f->open_object_section("OSDMapGate");
  f->dump_int("epoch", epoch);
  f->close_section();
}

seastar::future<epoch_t> OSDMapGate::OSDMapBlocker::block_op(OperationRef op)
{
  op->add_blocker(this);
  return promise.get_shared_future().then([=](epoch_t got_epoch) {
    op->clear_blocker(this);
    return seastar::make_ready_future<epoch_t>(got_epoch);
  });
}

seastar::future<epoch_t> OSDMapGate::wait_for_map(OperationRef op, epoch_t epoch)
{
  if (current >= epoch) {
    return seastar::make_ready_future<epoch_t>(current);
  } else {
    logger().info("evt epoch is {}, i have {}, will wait", epoch, current);
    auto fut = waiting_peering.insert(
      make_pair(epoch, OSDMapBlocker(blocker_type, epoch))).first->second.block_op(op);
    return shard_services.osdmap_subscribe(current, true).then(
      [fut=std::move(fut)]() mutable {
      return std::move(fut);
    });
  }
}

void OSDMapGate::got_map(epoch_t epoch) {
  current = epoch;
  auto first = waiting_peering.begin();
  auto last = waiting_peering.upper_bound(epoch);
  std::for_each(first, last, [epoch, this](auto& blocked_requests) {
    blocked_requests.second.promise.set_value(epoch);
  });
  waiting_peering.erase(first, last);
}

}
