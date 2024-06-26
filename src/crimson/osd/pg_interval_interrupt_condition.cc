// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_interval_interrupt_condition.h"
#include "pg.h"

#include "crimson/common/log.h"

SET_SUBSYS(osd);

namespace crimson::interruptible {
template thread_local interrupt_cond_t<crimson::osd::IOInterruptCondition>
interrupt_cond<crimson::osd::IOInterruptCondition>;
}

namespace crimson::osd {

IOInterruptCondition::IOInterruptCondition(Ref<PG>& pg)
  : pg(pg), pgid(pg->get_pgid()), e(pg->get_osdmap_epoch()) {}

IOInterruptCondition::IOInterruptCondition(disable_interval_t, Ref<PG>& pg)
  : pg(pg) {}

IOInterruptCondition::~IOInterruptCondition() {
  // for the sake of forward declaring PG (which is a detivate of
  // intrusive_ref_counter<...>)
}

bool IOInterruptCondition::new_interval_created() {
  LOG_PREFIX(IOInterruptCondition::new_interval_created);
  if (!e) {
    DEBUGDPP("skipping interval check, e is nullopt", *pg);
    return false;
  }
  const epoch_t interval_start = pg->get_interval_start_epoch();
  bool ret = *e < interval_start;
  if (ret) {
    DEBUGDPP("stored interval e{} < interval_start e{}", *pg, e, interval_start);
  }
  return ret;
}

bool IOInterruptCondition::is_stopping() {
  LOG_PREFIX(IOInterruptCondition::is_stopping);
  if (pg->stopping) {
    DEBUGDPP("pg stopping", *pg);
  }
  return pg->stopping;
}

bool IOInterruptCondition::is_primary() {
  return pg->is_primary();
}

} // namespace crimson::osd
