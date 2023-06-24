// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/not_before_queue.h"

#include "osd/osd_types.h"

namespace crimson::osd::scrub {

/// Represents pg queued for scrub
class scrub_schedule_item_t {
public:
  bool operator<(const scrub_schedule_item_t &rhs) {
    return true;
  }

  friend spg_t project_removal_class(const scrub_schedule_item_t&);
  friend utime_t project_not_before(const scrub_schedule_item_t&);
};

inline spg_t project_removal_class(const scrub_schedule_item_t&) {
  return spg_t{}; // TODO
}

inline utime_t project_not_before(const scrub_schedule_item_t&) {
  return utime_t{}; // TODO
}

/**
 * Scheduler
 *
 * Component responsible for OSD-wide scheduling of PG scrubs.
 */
class Scheduler {
  not_before_queue_t<scrub_schedule_item_t> queue;
public:
};

}

