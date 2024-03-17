// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <spdk/bdev.h>
#include <spdk/nvmf.h>

#include "block_driver.h"

class NVMEOFHandler {
  spdk_nvmf_tgt *target = nullptr;
  spdk_nvmf_subsystem *discovery_subsystem = nullptr;

  struct poll_group_association {
    spdk_thread *thread = nullptr;
    spdk_nvmf_poll_group *group = nullptr;
  };
  std::vector<poll_group_association> poll_group_associations;
public:
  seastar::future<> run();
  seastar::future<> stop();
};
