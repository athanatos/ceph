// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <atomic>

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
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

    bool do_stop = false;
    std::optional<seastar::future<>> on_stop;

    seastar::future<> do_poll();

  public:
    seastar::future<> start();
    seastar::future<> stop();
    void add_thread(seastar_spdk_thread_t *thread);
  };

  seastar::sharded<reactor_core_t> reactor_threads;
  std::atomic<unsigned> next_core;

public:
  seastar::future<> start();
  seastar::future<> stop();
  void add_thread(seastar_spdk_thread_t *thread);
};

static seastar_spdk_reactor_t *g_reactor = nullptr;

seastar::future<> seastar_spdk_reactor_t::reactor_core_t::stop()
{
  if (!on_stop) {
    return seastar::now();
  }
  do_stop = true;
  auto f = std::move(*on_stop);
  on_stop = std::nullopt;
  return f;
}

seastar::future<> seastar_spdk_reactor_t::reactor_core_t::start()
{
  on_stop = do_poll();
  return seastar::now();
}

seastar::future<> seastar_spdk_reactor_t::reactor_core_t::do_poll()
{
  while (!do_stop) {
    for (auto &&i: threads) {
      spdk_thread *t = spdk_thread_get_from_ctx(static_cast<void*>(&i));
      spdk_thread_poll(t, 0, 0);
      // TODO, return value is whether work was done, how to I find out whether the
      // thread is dead?  spdk_thread_is_exited/destroy?
    }
    co_await seastar::yield();
  }
}

void seastar_spdk_reactor_t::reactor_core_t::add_thread(
  seastar_spdk_thread_t *thread)
{
  threads.push_back(*thread);
}

seastar::future<> seastar_spdk_reactor_t::start()
{
  co_await reactor_threads.start();
  co_await reactor_threads.invoke_on_all(
    [](auto &core) {
      return core.start();
    });
}

seastar::future<> seastar_spdk_reactor_t::stop()
{
  return reactor_threads.stop();
}

void seastar_spdk_reactor_t::add_thread(
  seastar_spdk_thread_t *thread)
{
  std::ignore = reactor_threads.invoke_on(
    ++next_core % seastar::smp::count,
    [thread](auto &core) {
      core.add_thread(thread);
      return seastar::now();
    });
}

static int schedule_thread(struct spdk_thread *thread)
{
  auto *reactor_thread = new(spdk_thread_get_ctx(thread)) seastar_spdk_thread_t;
  g_reactor->add_thread(reactor_thread);
  return 0;
}

seastar::future<> spdk_reactor_start()
{
  /* info on running as non-root in a container:
   * https://stackoverflow.com/questions/66571932/can-you-run-dpdk-in-a-non-privileged-container-as-as-non-root-user/69178969#comment122283933_69178969
   * Basically:
   * - use va for iommu backed io addrs rather than the (default) pa, which requires privileges
   * - mount hugetlbfs somewhere other than /sys/kernel/mm/hugepages and reuser to container user
   *
   * TODOSAM: current status is that I'm still trying to work out how to either pass --no-huge
   * or actually make hugepages work.
   *
   * For the former, it seems that lib/env_dpdk/init.c's interpretation of spdk_env_opts
   * and spdk_env_opts::env_context in particular is a bit odd, haven't quite got it to
   * parse multiple arguments yet.
   *
   * For the latter, I've mounted hugetlbfs, but I'm missing something about how spdk is discovering
   * hugepages status.  Also, it seems to want 1GB pages rather than the 2MB ones this workstation is
   * configured with at present.
   */


  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  // this doesn't seem to be working -- typos in the arguments don't trigger the invalid argument
  // behavior
  const char *args[] = {
    "--iova-mde=va",
    "--no-huge",
    "--in-memory",
    nullptr
  };
  opts.env_context = (void*)args;

  // pa mode basically requires root and doesn't play nicely with containers
  // setting iova_mode here actually does work
  //opts.iova_mode = "va";

  // presumably, this has been mounted and chown'd to the right user
  // TODOSAM obviously, this needs better configuration
  // this also appears to influence behavior
  //opts.hugedir = "/home/sam/tmp/hugepages";

  // I guess env_context is the set of command line args usually passed to the eal?
#if 0
#endif

  int r = spdk_env_init(&opts);
  assert(r == 0);

  // spdk_env_dpdk_post_init(false); ?

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
