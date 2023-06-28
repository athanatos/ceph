// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/timer.hh>

#include "crimson/common/smp_helpers.h"

namespace crimson::common {

/**
 * PeriodicExecutor
 *
 * Executes lambda on a configurable interval.  Using seastar::timer directly
 * has a few pitfalls this implementation attempts to avoid:
 * - Need to ensure executions do not overlap, optional period argument to
 *   seastar::timer::arm may end causing overlapping executions if execution
 *   time is too long.
 * - stop() needs to ensure that any prior executions have completed, otherwise
 *   the function may live through OSD::restart() or OSD::stop().
 */
struct PeriodicExecutor {
  using clock_t = seastar::lowres_clock;

  enum class state_t {
    STOPPED,
    WAITING,
    EXECUTING,
    STOPPING
  } state = state_t::STOPPED;

  const clock_t::duration period;
  const core_id_t core;
  std::optional<seastar::promise<>> stopping;
  seastar::timer<clock_t> timer;
public:

  template <typename F>
  PeriodicExecutor(clock_t::duration _period, F &&f)
    : period{_period},
      core{seastar::this_shard_id()},
      timer{[this, f=std::forward<F>(f)] {
	if (state == state_t::STOPPED) {
	  return;
	}
	state = state_t::EXECUTING;
	std::ignore = std::invoke(
	  f
	).then([this] {
	  ceph_assert(state != state_t::STOPPED);
	  ceph_assert(state != state_t::WAITING);
	  if (state == state_t::EXECUTING) {
	    state = state_t::WAITING;
	    timer.arm(period);
	  } else {
	    ceph_assert(state == state_t::STOPPING);
	    ceph_assert(stopping);
	    stopping->set_value();
	  }
	});
      }} {}

  void start() {
    ceph_assert(core == seastar::this_shard_id());
    ceph_assert(state == state_t::STOPPED);
    state = state_t::WAITING;
    timer.arm(period);
  }

  /// Blocks until timer is canceled and pending executions are complete
  seastar::future<> stop() {
    ceph_assert(core == seastar::this_shard_id());
    ceph_assert(state != state_t::STOPPED);
    ceph_assert(state != state_t::STOPPING);

    timer.cancel();
    if (state == state_t::WAITING) {
      state = state_t::STOPPED;
      return seastar::now();
    } else {
      ceph_assert(state == state_t::EXECUTING);
      ceph_assert(!stopping);
      stopping = seastar::promise<>();
      return stopping->get_future(
      ).then([this] {
	stopping = std::nullopt;
	state = state_t::STOPPED;
	return seastar::now();
      });
    }
  }
};

}
