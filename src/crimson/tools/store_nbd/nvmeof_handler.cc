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
}

seastar::future<> NVMEOFHandler::stop()
{
  return spdk_reactor.stop();
}
