// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::segment_manager {

EphemeralSegment::EphemeralSegment(
  EphemeralSegmentManager &manager, segment_id_t id)
  : manager(manager), id(id) {}

Segment::close_ertr::future<> EphemeralSegment::close()
{
  manager.close_segment(id);
  return close_ertr::now();
}

Segment::write_ertr::future<> EphemeralSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset + bl.length() >= manager.config.segment_size)
    return crimson::ct_error::enospc::make();
  return write_ertr::now();
}

EphemeralSegmentManager::EphemeralSegmentManager(ephemeral_config_t config)
  : config(config) {}

Segment::close_ertr::future<> EphemeralSegmentManager::close_segment(segment_id_t id)
{
  return Segment::close_ertr::now();
}

EphemeralSegmentManager::init_ertr::future<> EphemeralSegmentManager::init()
{
  logger().debug(
    "Initing ephemeral segment manager with config {}",
    config);
    
  if (config.block_size % (4<<10) != 0) {
    return crimson::ct_error::invarg::make();
  }
  if (config.segment_size % config.block_size != 0) {
    return crimson::ct_error::invarg::make();
  }
  if (config.size % config.segment_size != 0) {
    return crimson::ct_error::invarg::make();
  }
  
  auto addr = ::mmap(
    nullptr,
    config.size,
    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
    -1,
    0);

  if (addr == MAP_FAILED)
    return crimson::ct_error::enospc::make();

  buffer = (char*)addr;
  return init_ertr::now();
}

SegmentManager::open_ertr::future<SegmentRef> EphemeralSegmentManager::open(
  segment_id_t id)
{
  return open_ertr::make_ready_future<SegmentRef>();
}

SegmentManager::release_ertr::future<> EphemeralSegmentManager::release(
  segment_id_t id)
{
  return release_ertr::now();
}

SegmentManager::read_ertr::future<ceph::bufferlist> EphemeralSegmentManager::read(
  paddr_t addr, size_t len)
{
  return read_ertr::make_ready_future<ceph::bufferlist>();
}

}
