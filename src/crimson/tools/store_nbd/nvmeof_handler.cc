// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <spdk/stdinc.h>
#include <spdk/env.h>
#include <spdk/env_dpdk.h>
#include <spdk/thread.h>
#include <spdk/rpc.h>
#include <spdk_internal/event.h>

#include "crimson/common/seastar_spdk_reactor.h"

#include "nvmeof_handler.h"

// borrowed liberally from examples/nvmf/nvmf.c

void fulfill_promise_subsystem(spdk_nvmf_subsystem *, void *p, int r)
{
  assert(r == 0);
  static_cast<seastar::promise<>*>(p)->set_value();
}

void fulfill_promise(int r, void *p)
{
  assert(r == 0);
  static_cast<seastar::promise<>*>(p)->set_value();
}

struct create_poll_group_ret_t {
  spdk_nvmf_tgt *tgt;

  spdk_nvmf_poll_group **group;
  seastar::promise<> cont;
};
void create_poll_group(void *p)
{
  auto &ret = *static_cast<create_poll_group_ret_t*>(p);
  *ret.group = spdk_nvmf_poll_group_create(ret.tgt);
  ret.cont.set_value();
}


static int accept_poll(void *p)
{
  auto *tgt = static_cast<spdk_nvmf_tgt*>(p);
  spdk_nvmf_tgt_accept(tgt);
  return -1;
}
struct start_poller_ret_t {
  spdk_nvmf_tgt *tgt;

  spdk_poller **poller;
  seastar::promise<> cont;
};

void start_poller(void *p)
{
  constexpr unsigned poll_rate = 10000; /* 10ms */
  auto &ret = *static_cast<start_poller_ret_t*>(p);
  *(ret.poller) = SPDK_POLLER_REGISTER(
    accept_poll, &ret.tgt,
    poll_rate);
  ret.cont.set_value();
}

seastar::future<> NVMEOFHandler::run()
{
  co_await spdk_reactor_start();

  // initialize bdev layer
  {
    seastar::promise<> p;
    spdk_subsystem_init(fulfill_promise, static_cast<void*>(&p));
    co_await p.get_future();
  }

  {
    /* See https://spdk.io/doc/nvmf_tgt_pg.html.  An spdk_nvmf_tgt is the
     * top level container for a collection of subsystems which in turn
     * contain namespaces which map to spdk bdev's. */
    spdk_nvmf_target_opts tgt_opts;
    tgt_opts.max_subsystems = 2; // discovery + actual target? TODO
    snprintf(tgt_opts.name, sizeof(tgt_opts.name), "%s", "nvmf_example");

    // Default nvmeof target
    nvmf_tgt = spdk_nvmf_tgt_create(&tgt_opts);
    if (nvmf_tgt == NULL) {
      std::cerr << "spdk_nvmf_tgt_create() failed" << std::endl;
      assert(0 == "cannot create target");
    }
    
    /* Create the special discovery subsystem responsible for exposing to
     * hosts the subsystems exposing namespaces within the target. */
    spdk_nvmf_subsystem *subsystem = spdk_nvmf_subsystem_create(
      nvmf_tgt,
      SPDK_NVMF_DISCOVERY_NQN,      // NQN
      SPDK_NVMF_SUBTYPE_DISCOVERY,  // subsystem type
      0);                           // number of namespaces, 0 due to discovery?

    if (subsystem == NULL) {
      std::cerr << "failed to create discovery nvmf library subsystem"
		<< std::endl;
      assert(0 == "cannot create subsystem");
    }
    
    /* Allow any host to access the discovery subsystem */
    spdk_nvmf_subsystem_set_allow_any_host(subsystem, true);
    
    std::cout << "created a nvmf target service" << std::endl;
  }

  spdk_thread *thread = nullptr;
  spdk_nvmf_poll_group *group = nullptr;
  {
    create_poll_group_ret_t poll_thread{nvmf_tgt, &group};
    spdk_cpuset cpuset;
    thread = spdk_thread_create("poll_thread", &cpuset);
    spdk_thread_send_msg(thread, create_poll_group, &poll_thread);
    co_await poll_thread.cont.get_future();
  }

  {
    spdk_nvmf_subsystem *subsystem = spdk_nvmf_subsystem_get_first(nvmf_tgt);
    while (subsystem) {
      seastar::promise<> p;
      spdk_nvmf_subsystem_start(subsystem, fulfill_promise_subsystem, &p);
      co_await p.get_future();
      subsystem = spdk_nvmf_subsystem_get_next(subsystem);
    }
  }

  spdk_poller *poller = nullptr;
  {
    start_poller_ret_t ret{nvmf_tgt, &poller};
    spdk_thread_send_msg(thread, start_poller, &ret);
    co_await ret.cont.get_future();
  }
}

seastar::future<> NVMEOFHandler::stop()
{
  return spdk_reactor_stop();
}
