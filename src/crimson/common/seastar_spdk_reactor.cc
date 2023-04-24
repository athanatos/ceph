// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar_spdk_reactor.h"

static int schedule_thread(struct spdk_thread *thread)
{
  return 0;
}

#if 0
// should be elsewhere
static void start_rpc(int rc, void *arg)
{
  spdk_rpc_initialize("/var/tmp/spdk.sock");
  spdk_rpc_set_state(SPDK_RPC_RUNTIME);
}
spdk_rpc_finish();
#endif

seastar::future<> SeastarSPDKReactor::start()
{
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  if (spdk_env_init(&opts) == -EALREADY) {
    spdk_env_dpdk_post_init(false);
  }
  spdk_thread_lib_init(schedule_thread, 0);
  return seastar::now();
}

seastar::future<> SeastarSPDKReactor::stop()
{
  //spdk_subsystem_fini(subsystem_fini_done, &threads);
  return seastar::now();
}
