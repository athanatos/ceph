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
  manager.segment_close(id);
  return close_ertr::now();
}

Segment::write_ertr::future<> EphemeralSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.config.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() >= manager.config.segment_size)
    return crimson::ct_error::enospc::make();
  
  return write_ertr::now();
}

EphemeralSegmentManager::EphemeralSegmentManager(ephemeral_config_t config)
  : config(config) {}

Segment::close_ertr::future<> EphemeralSegmentManager::segment_close(segment_id_t id)
{
  return Segment::close_ertr::now();
}

Segment::close_ertr::future<> segment_write(
  paddr_t addr,
  ceph::bufferlist bl)
{
  if (addr.segment >= get_num_segments())
    return crimson::ct_error::invarg::make();

  bl.copy_out(0, bl.length(), buffer + get_offset(addr));
  return Segment::write_ertr::now();
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

  segment_state.resize(config.size / config.segment_size, segment_state_t::EMPTY);

  if (addr == MAP_FAILED)
    return crimson::ct_error::enospc::make();

  buffer = (char*)addr;
  return init_ertr::now();
}

SegmentManager::open_ertr::future<SegmentRef> EphemeralSegmentManager::open(
  segment_id_t id)
{
  if (id >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (segment_state[id] != segment_state_t::EMPTY)
    return crimson::ct_error::invarg::make();

  segment_state[id] = segment_state_t::OPEN;
  return open_ertr::make_ready_future<SegmentRef>(new EphemeralSegment(*this, id));
}

SegmentManager::release_ertr::future<> EphemeralSegmentManager::release(
  segment_id_t id)
{
  if (id >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (segment_state[id] != segment_state_t::CLOSED)
    return crimson::ct_error::invarg::make();

  return release_ertr::now();
}

SegmentManager::read_ertr::future<ceph::bufferlist> EphemeralSegmentManager::read(
  paddr_t addr, size_t len)
{
  if (addr.segment >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (addr.offset + len >= config.segment_size)
    return crimson::ct_error::invarg::make();

  ceph::bufferptr ptr(buffer + get_offset(addr), len);
  ceph::bufferlist bl;
  bl.append(ptr);
  return read_ertr::make_ready_future<ceph::bufferlist>(std::move(bl));
}

}
