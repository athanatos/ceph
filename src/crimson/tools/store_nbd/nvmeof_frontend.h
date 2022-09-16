// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdint>

#include <seastar/core/future-util.hh>

#include "crimson/common/log.h"

#include "iofrontend.h"

/**
 * NVMEOFFrontend
 *
 * nvmeof target which forwards IO to BlockDriver
 */
class NVMEOFFrontend : public IOFrontend {
public:
  NVMEOFFrontend(
    BlockDriver &backend,
    config_t config) {}

  void run();
  seastar::future<> stop();
};




