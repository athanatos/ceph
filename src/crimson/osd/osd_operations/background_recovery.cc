// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/background_recovery.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

BackgroundRecovery::BackgroundRecovery(
  ShardServices &ss,
  Ref<PG> pg,
  epoch_t epoch_started,
  ceph::osd::scheduler::scheduler_class_t scheduler_class)
  : ss(ss), pg(pg), epoch_started(epoch_started),
    scheduler_class(scheduler_class)
{}

seastar::future<bool> BackgroundRecovery::do_recovery()
{
  if (pg->has_reset_since(epoch_started))
    return seastar::make_ready_future<bool>(false);
  return with_blocking_future(
    pg->start_recovery_ops(5 /* replace with config */));
}

void BackgroundRecovery::print(std::ostream &lhs) const
{
  lhs << "BackgroundRecovery(" << pg->get_pgid() << ")";
}

void BackgroundRecovery::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
  {
    f->open_object_section("recovery_detail");
    pg->dump_recovery_state(f);
    f->close_section();
  }
}

seastar::future<> BackgroundRecovery::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  return ss.throttler.with_throttle_while(
    this, get_scheduler_params(), [ref, this]() -> auto {
      return do_recovery();
    });
}

}
