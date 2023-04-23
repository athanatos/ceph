// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

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

class SeastarSPDKReactor {
  struct seastar_lw_thread_t {
    struct spdk_thread *thread;
    seastar::promise<> p;
    std::optional<seastar::reactor::poller> poller;
    seastar_lw_thread_t() : thread(NULL) {}
    void start() {
      std::string thread_name("thread");
      thread_name += std::to_string(seastar::this_shard_id());
      thread = spdk_thread_create(thread_name.c_str(), NULL);
      spdk_set_thread(thread);
    }
    bool poll_spdk_thread() {
      spdk_thread_poll(thread, 0, spdk_get_ticks());
      return true;
    }
    seastar::future<> run() {
      poller = seastar::reactor::poller::simple([&] { return poll_spdk_thread(); });
      return p.get_future();
    }
    seastar::future<> stop() {
      p.set_value();
      return seastar::now();
    }
    seastar::future<> destroy() {
      spdk_thread_exit(thread);
      spdk_thread_destroy(thread);
      return seastar::now();
    }
  };
  seastar::distributed<seastar_lw_thread_t> threads;
public:
  friend void subsystem_fini_done(void*);
  seastar::future<> start();
  seastar::future<> stop();
};
