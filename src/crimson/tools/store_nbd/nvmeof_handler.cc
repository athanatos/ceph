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

void fulfill_promise(int r, void *p)
{
  assert(r == 0);
  static_cast<seastar::promise<>*>(p)->set_value();
}

seastar::future<> NVMEOFHandler::run()
{
  co_await spdk_reactor.start();

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
      std::cerr << "failed to create discovery nvmf library subsystem" << std::endl;
      assert(0 == "cannot create subsystem");
    }
    
    /* Allow any host to access the discovery subsystem */
    spdk_nvmf_subsystem_set_allow_any_host(subsystem, true);
    
    std::cout << "created a nvmf target service" << std::endl;
  }
}

seastar::future<> NVMEOFHandler::stop()
{
  return spdk_reactor.stop();
}
