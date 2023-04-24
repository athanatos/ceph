// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <spdk/bdev.h>
#include <spdk/nvmf.h>

#include "block_driver.h"

class NVMEOFHandler {
  SeastarSPDKReactor spdk_reactor;
  spdk_nvmf_tgt *nvmf_tgt;
public:
  seastar::future<> run();
  seastar::future<> stop();
};
