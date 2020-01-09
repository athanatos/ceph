// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "crimson/os/seastore/segment_manager.h"

#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace crimson::os::seastore::segment_manager {

class EphemeralSegment final : public Segment {
public:

  close_ertr::future<> close() final;
  write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) final;

  ~EphemeralSegment() {}
};

class EphemeralSegmentManager final : public SegmentManager {
  const ephemeral_config_t config;

  char *buffer = nullptr;
public:
  EphemeralSegmentManager(ephemeral_config_t config);

  
  using init_ertr = crimson::errorator<
    crimson::ct_error::enospc,
    crimson::ct_error::invarg,
    crimson::ct_error::erange>;
  init_ertr::future<> init();

  open_ertr::future<SegmentRef> open(segment_id_t id) final;

  release_ertr::future<> release(segment_id_t id) final;

  read_ertr::future<ceph::bufferlist> read(
    paddr_t addr, size_t len) final;
};
    
}
