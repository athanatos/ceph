// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

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

class SeastarSPDKReactor {
  /// intrusive entry into reactor thread list
  struct reactor_thread_t : boost::intrusive::list_base_hook<> {};
  using reactor_thread_list_t = boost::intrusive::list<reactor_thread_t>;

public:
  friend void subsystem_fini_done(void*);
  seastar::future<> start();
  seastar::future<> stop();
};
