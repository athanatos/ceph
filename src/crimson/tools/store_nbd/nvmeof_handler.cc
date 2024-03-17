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

void run_wrapped_function(void *_f)
{
  auto f = static_cast<std::function<void()>*>(_f);
  std::invoke(*f);
}

template <typename F>
seastar::future<> run_on_thread(spdk_thread *thread, F &&f)
{
  seastar::promise<> p;
  std::function<void()> wrapped([f=std::forward<F>(f), &p] {
    std::invoke(f);
    p.set_value();
  });
  spdk_thread_send_msg(thread, run_wrapped_function, &wrapped);
  co_await p.get_future();
}

seastar::future<> NVMEOFHandler::run()
{
  co_await spdk_reactor_start();

  // initialize bdev layer (NVMF_INIT_SUBSYSTEM from example above)
  {
    seastar::promise<> p;
    spdk_subsystem_init(fulfill_promise, static_cast<void*>(&p));
    co_await p.get_future();
  }

  /* TODO: examples/nvmf.c at this point initializes the rpc interface --
   * figure out how to configure */

  // initialize target (NVMF_INIT_TARGET from example above)
  {
    /* See https://spdk.io/doc/nvmf_tgt_pg.html.  An spdk_nvmf_tgt is the
     * top level container for a collection of subsystems which in turn
     * contain namespaces which map to spdk bdev's. */
    spdk_nvmf_target_opts tgt_opts;
    tgt_opts.max_subsystems = 2; // discovery + actual target? TODO
    snprintf(tgt_opts.name, sizeof(tgt_opts.name), "%s", "store_nvmeof_handler");

    // Default nvmeof target
    target = spdk_nvmf_tgt_create(&tgt_opts);
    if (target == NULL) {
      std::cerr << "spdk_nvmf_tgt_create() failed" << std::endl;
      assert(0 == "cannot create target");
    }
    
    /* Create the special discovery subsystem responsible for exposing to
     * hosts the subsystems exposing namespaces within the target. */
    discovery_subsystem = spdk_nvmf_subsystem_create(
      target,
      SPDK_NVMF_DISCOVERY_NQN,      // NQN
      SPDK_NVMF_SUBTYPE_DISCOVERY,  // subsystem type
      0);                           // number of namespaces, 0 due to discovery?

    if (discovery_subsystem == NULL) {
      std::cerr << "failed to create discovery nvmf library subsystem"
		<< std::endl;
      assert(0 == "cannot create subsystem");
    }
    
    /* Allow any host to access the discovery subsystem */
    spdk_nvmf_subsystem_set_allow_any_host(discovery_subsystem, true);
    
    std::cout << "created a nvmeof target service" << std::endl;
  }

  // initialize poll groups (NVMF_INIT_POLL_GROUPS from example above)
  {
    struct spdk_cpuset tmp_cpumask = {};
    uint32_t i;
    char thread_name[32];
    struct spdk_thread *thread;
    
    SPDK_ENV_FOREACH_CORE(i) {
      spdk_cpuset_zero(&tmp_cpumask);
      spdk_cpuset_set_cpu(&tmp_cpumask, i, true);
      snprintf(
	thread_name, sizeof(thread_name),
	"store_nvmeof_poll_group_%u", i);
      
      thread = spdk_thread_create(thread_name, &tmp_cpumask);
      assert(thread != NULL);
      
      poll_group_associations.push_back(poll_group_association{});
      co_await run_on_thread(
	thread,
	[this, &pga = poll_group_associations.back()] {
	  pga.thread = spdk_get_thread();
	  pga.group = spdk_nvmf_poll_group_create(target);
	  if (!pga.group) {
	    std::cerr << "failed to create poll group" << std::endl;
	    assert(0 == "unable to create poll group");
	  }
	});
    }
  }

  // initialize poll groups (NVMF_INIT_START_SUBSYSTEMS from example above)
  // TODO where do the other subsystems come from?
  {
    spdk_nvmf_subsystem *subsystem = spdk_nvmf_subsystem_get_first(target);
    while (subsystem) {
      seastar::promise<> p;
      spdk_nvmf_subsystem_start(subsystem, fulfill_promise_subsystem, &p);
      co_await p.get_future();
      subsystem = spdk_nvmf_subsystem_get_next(subsystem);
    }
  }

  // start acceptor (NVMF_START_ACCEPTOR from example above)
  /**
   * see lib/nvmf/nvmf_rpc.c, includes handlers for rpc commands in
   * examples/nvmf/README.md:
   * - "nvmf_create_transport"
   *   - rpc_nvmf_create_transport
   *   - spdk_nvme_transport_id_parse_trtype (transport type -> trtype)
   *     - spdk/nvme.h
   *   - spdk_nvmf_transport_opts_init (fills in ops)
   *   - spdk_nvmf_transport_create
   *   - spdk_nvmf_add_transport
   * - "nvmf_create_subsystem"
   *   - spdk_nvmf_subsystem_create
   *   - spdk_nvmf_subsystem_set_allow_any_host
   *   - spdk_nvmf_subsystem_start
   * - "bdev_malloc_create"
   *   - TODO, need to figure out how to fill in a bdev
   * - "nvmf_subsystem_add_ns"
   *   - spdk_nvmf_ns_opts_get_defaults
   *   - spdk_nvmf_subsystem_add_ns
   * - "nvmf_subsystem_add_listener"
   *   - spdk_nvmf_subsystem_add_listener
   *
   * The bdev thing looks pretty straightforward, see bdev_rbd
   */
}

seastar::future<> NVMEOFHandler::stop()
{
  return spdk_reactor_stop();
}
