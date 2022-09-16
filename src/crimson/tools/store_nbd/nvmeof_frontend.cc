// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <spdk/thread.h>

#include "nvmeof_frontend.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

void NVMEOFFrontend::run()
{
}


seastar::future<> NVMEOFFrontend::stop()
{
  return seastar::now();
}
