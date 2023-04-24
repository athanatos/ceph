// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <spdk/stdinc.h>
#include <spdk/env.h>
#include <spdk/env_dpdk.h>
#include <spdk/thread.h>
#include <spdk/rpc.h>
#include <spdk_internal/event.h>

#include "seastar_spdk_reactor.h"

struct seastar_spdk_thread_t : boost::intrusive::list_base_hook<> {};
using seastar_spdk_thread_list_t = boost::intrusive::list<seastar_spdk_thread_t>;

class seastar_spdk_reactor_t {
  class reactor_core_t {
    seastar_spdk_thread_list_t threads;
  public:
    seastar::future<> stop();
    seastar::future<> do_poll();
    void add_thread(seastar_spdk_thread_t *thread);
  };
  seastar::sharded<reactor_core_t> reactor_threads;

public:
  seastar::future<> start();
  seastar::future<> stop();
  void add_thread(seastar_spdk_thread_t *thread);
};

static seastar_spdk_reactor_t *g_reactor = nullptr;

seastar::future<> seastar_spdk_reactor_t::reactor_core_t::stop()
{
  return seastar::now(); // TODO
}

seastar::future<> seastar_spdk_reactor_t::reactor_core_t::do_poll()
{
  for (auto &&i: threads) {
    spdk_thread *t = spdk_thread_get_from_ctx(static_cast<void*>(&i));
    spdk_thread_poll(t, 0, 0);
    // TODO, return value is whether work was done, how to I find out whether the
    // thread is dead?  spdk_thread_is_exited/destroy?
  }
  return seastar::now();
}

void seastar_spdk_reactor_t::reactor_core_t::add_thread(
  seastar_spdk_thread_t *thread)
{
}

seastar::future<> seastar_spdk_reactor_t::start()
{
  return reactor_threads.start();
}

seastar::future<> seastar_spdk_reactor_t::stop()
{
  return reactor_threads.stop();
}

void seastar_spdk_reactor_t::add_thread(
  seastar_spdk_thread_t *thread)
{
}

static int schedule_thread(struct spdk_thread *thread)
{
  auto *reactor_thread = new(spdk_thread_get_ctx(thread)) seastar_spdk_thread_t;
  g_reactor->add_thread(reactor_thread);
  return 0;
}

seastar::future<> spdk_reactor_start()
{
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  if (spdk_env_init(&opts) == -EALREADY) {
    spdk_env_dpdk_post_init(false);
  }

  assert(nullptr == g_reactor);
  g_reactor = new seastar_spdk_reactor_t;

  spdk_thread_lib_init(schedule_thread, sizeof(seastar_spdk_thread_t));
  return g_reactor->start();
}

seastar::future<> spdk_reactor_stop()
{
  co_await g_reactor->stop();
  delete g_reactor;
}
