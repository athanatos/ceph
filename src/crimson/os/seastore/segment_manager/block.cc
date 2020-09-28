// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/segment_manager/block.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::segment_manager {

BlockSegment::BlockSegment(
  BlockSegmentManager &manager, segment_id_t id)
  : manager(manager), id(id) {}

segment_off_t BlockSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

Segment::close_ertr::future<> BlockSegment::close()
{
  manager.segment_close(id);
  return close_ertr::now();
}

Segment::write_ertr::future<> BlockSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.config.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() > manager.config.segment_size)
    return crimson::ct_error::enospc::make();

  return manager.segment_write({id, offset}, bl);
}

Segment::close_ertr::future<> BlockSegmentManager::segment_close(segment_id_t id)
{
  if (segment_state[id] != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  segment_state[id] = segment_state_t::CLOSED;
  return Segment::close_ertr::now();
}

Segment::write_ertr::future<> BlockSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  logger().debug(
    "segment_write to segment {} at offset {}, physical offset {}, len {}, crc {}",
    addr.segment,
    addr.offset,
    get_offset(addr),
    bl.length(),
    bl.crc32c(1));
  if (!ignore_check && segment_state[addr.segment] != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  bl.begin().copy(bl.length(), buffer + get_offset(addr));

  // DEBUGGING, remove
  bufferptr out(bl.length());
  out.copy_in(0, bl.length(), buffer + get_offset(addr));
  bufferlist bl2;
  bl2.append(out);
  assert(bl2.crc32c(1) == bl.crc32c(1));

  return Segment::write_ertr::now();
}

BlockSegmentManager::~BlockSegmentManager()
{
}

SegmentManager::open_ertr::future<SegmentRef> BlockSegmentManager::open(
  segment_id_t id)
{
  if (id >= get_num_segments()) {
    logger().error("BlockSegmentManager::open: invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  return open_ertr::make_ready_future<SegmentRef>(nullptr);
}

SegmentManager::release_ertr::future<> BlockSegmentManager::release(
  segment_id_t id)
{
  logger().debug("BlockSegmentManager::release: {}", id);

  if (id >= get_num_segments()) {
    logger().error(
      "BlockSegmentManager::release: invalid segment {}",
      id);
    return crimson::ct_error::invarg::make();
  }

  if (segment_state[id] != segment_state_t::CLOSED) {
    logger().error(
      "BlockSegmentManager::release: segment id {} not closed",
      id);
    return crimson::ct_error::invarg::make();
  }

  return release_ertr::now();
}

SegmentManager::read_ertr::future<> BlockSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  if (addr.segment >= get_num_segments()) {
    logger().error(
      "BlockSegmentManager::read: invalid segment {}",
      addr);
    return crimson::ct_error::invarg::make();
  }

  if (addr.offset + len > config.segment_size) {
    logger().error(
      "BlockSegmentManager::read: invalid offset {}~{}!",
      addr,
      len);
    return crimson::ct_error::invarg::make();
  }

  return read_ertr::now();
}

}
