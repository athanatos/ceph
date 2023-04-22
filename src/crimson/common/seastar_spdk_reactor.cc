// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar_spdk_reactor.h"

static int seastar_schedule_thread(struct spdk_thread *thread)
{
  return 0;
}

static void start_rpc(int rc, void *arg)
{
  spdk_rpc_initialize("/var/tmp/spdk.sock");
  spdk_rpc_set_state(SPDK_RPC_RUNTIME);
}

seastar::future<> SeastarSPDKReactor::start()
{
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  if (spdk_env_init(&opts) == -EALREADY) {
    spdk_env_dpdk_post_init(false);
  }
  return seastar::async([&] {
    spdk_thread_lib_init(seastar_schedule_thread, 0);
    threads.start().get0();
    threads.invoke_on_all([] (auto &thread) {
      return thread.start();
    }).get();
    spdk_subsystem_init(start_rpc, NULL);
    threads.invoke_on_all([] (auto &thread) {
      return thread.run();
    }).get();
    threads.stop().get();
  });

  return seastar::now();
}

static void subsystem_fini_done(void *arg)
{
  auto *threads = static_cast<seastar::distributed<seastar_lw_thread_t>*>(arg);
  threads->invoke_on_all([](auto &thread) {
    return thread.stop();
  }).get();
  threads->invoke_on_all([](auto &thread) {
    return thread.destroy();
  }).get();
}

seastar::future<> SeastarSPDKReactor::stop()
{
  spdk_rpc_finish();
  spdk_subsystem_fini(subsystem_fini_done, &threads);
  return seastar::now();
}
