// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <condition_variable>

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/ceph_time.h"

namespace ceph::common {

/**
 * intrusive_timer: timer implementation with allocation-free
 * schedule/cancel
 */
class intrusive_timer {
  using clock_t = ceph::coarse_real_clock;
  struct callback_base_t : boost::intrusive_ref_counter<callback_base_t>,
			   boost::intrusive::set_base_hook<> {
    
    clock_t::time_point schedule_point;
    unsigned incarnation = 0;

    callback_base_t(const callback_base_t &) = delete;
    callback_base_t(callback_base_t &&) = delete;
    callback_base_t &operator=(const callback_base_t &) = delete;
    callback_base_t &operator=(callback_base_t &&) = delete;

    void run(unsigned _incarnation) {
      if (_incarnation == incarnation) {
	assert(is_scheduled());
	++incarnation;
	_run();
      }
    }

    bool is_scheduled() const {
      return incarnation % 2 == 1;
    }

    void ready_for_schedule(auto time) {
      ceph_assert(!is_scheduled());
      schedule_point = time;
      ++incarnation;
    }

    void maybe_cancel() {
      if (is_scheduled()) ++incarnation;
    }

    virtual void _run() = 0;

    auto operator<=>(const callback_base_t &rhs) const {
      return std::make_pair(schedule_point, this) <=>
	std::make_pair(rhs.schedule_point, &rhs);
    }

    using ref = boost::intrusive_ptr<callback_base_t>;

    callback_base_t() = default;
    virtual ~callback_base_t() = default;
    unsigned get_incarnation() { return incarnation; }
  };

  template <typename Mutex, typename F>
  struct locked_callback_t final : callback_base_t {
    Mutex &m;
    F f;
    locked_callback_t(Mutex &m, F &&f) : m(m), f(std::forward<F>(f)) {}

    void _run() final {
      std::unique_lock l(m);
      std::invoke(f);
    }
  };

  class schedule_t {
    boost::intrusive::set<callback_base_t> events;

  public:
    void schedule(callback_base_t &cb, clock_t::time_point time) {
      cb.ready_for_schedule(time);
      intrusive_ptr_add_ref(&cb);
      events.insert(cb);
    }

    callback_base_t::ref peek() {
      return events.empty() ? nullptr : &*(events.begin());
    }

    void remove(callback_base_t &cb) {
      intrusive_ptr_release(&cb);
      events.erase(cb);
    }

    friend class thread_executor_t;
  };

public:
  class callback_t {
    callback_base_t::ref cb;
    friend class intrusive_timer;
  public:
    template <typename Mutex, typename F>
    callback_t(Mutex &m, F &&f)
      : cb(new locked_callback_t{m, std::forward<F>(f)}) {}

    bool is_scheduled() const {
      return cb->is_scheduled();
    }
  };

  class thread_executor_t {
    bool stopping = false;
    std::mutex lock;
    std::condition_variable cv;

    std::thread t;
    schedule_t queue;
  public:
    thread_executor_t() : t([this] { run(); }) {}

    void run() {
      std::unique_lock l(lock);
      while (true) {
	if (stopping) {
	  return;
	}

	auto next = queue.peek();
	if (!next) {
	  cv.wait(l);
	  continue;
	}

	if (next->schedule_point > clock_t::now()) {
	  cv.wait_until(l, next->schedule_point);
	  continue;
	}

	queue.remove(*next);
	auto incarnation = next->incarnation;
	l.unlock();
	next->run(incarnation);
	l.lock();
      }
    }

    template <typename T>
    void schedule_after(callback_t &cb, T after) {
      std::unique_lock l(lock);
      queue.schedule(*(cb.cb), clock_t::now() + after);
      cv.notify_one();
    }

    void cancel(callback_t &cb) {
      std::unique_lock l(lock);
      queue.remove(*(cb.cb));
      cb.cb->maybe_cancel();
    }

    void stop() {
      {
	std::unique_lock l(lock);
	stopping = true;
	cv.notify_one();
      }
      t.join();
    }
  };
};

}
