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

namespace crimson::os::seastore::segment_manager::block {

static constexpr size_t ALIGNMENT = 4096;

struct block_sm_superblock_t {
  size_t size = 0;
  size_t segment_size = 0;
  size_t block_size = 0;

  DENC(block_sm_superblock_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.segment_size, p);
    denc(v.block_size, p);
    DENC_FINISH(p);
  }
};

}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::segment_manager::block::block_sm_superblock_t
)

namespace crimson::os::seastore::segment_manager::block {

block_sm_superblock_t make_superblock(
  const BlockSegmentManager::mkfs_config_t &config,
  const seastar::stat_data &data)
{
  return block_sm_superblock_t{
    data.size,
    config.segment_size,
    data.block_size
  };
}

BlockSegmentManager::access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>
  >
open_device(std::string_view path, seastar::open_flags mode)
{
  return seastar::open_file_dma(path, mode).then([path](auto file) {
    return seastar::file_stat(path, seastar::follow_symlink::yes).then(
      [file=std::move(file)](auto stat) {
	return std::make_pair(std::move(file), stat);
      });
  });
}

BlockSegmentManager::access_ertr::future<>
write_superblock(seastar::file &device, block_sm_superblock_t sb)
{
  assert(ceph::encoded_sizeof_bounded<block_sm_superblock_t>() <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp) {
      bufferlist bl;
      ::encode(sb, bl);
      auto iter = bl.begin();
      assert(bl.length() < sb.block_size);
      iter.copy(bl.length(), bp.c_str());
      return device.dma_write(
	0,
	bp.c_str(),
	bp.length()
      ).then([sb](auto size) -> BlockSegmentManager::access_ertr::future<> {
	  if (size < sb.block_size) {
	    // seastar/.../file.hh indicates that a short write here means io
	    // error
	    return crimson::ct_error::input_output_error::make();
	  }
	  return BlockSegmentManager::access_ertr::now();
	});
    });
}

BlockSegmentManager::access_ertr::future<block_sm_superblock_t>
read_superblock(seastar::file &device, seastar::stat_data sd)
{
  assert(ceph::encoded_sizeof_bounded<block_sm_superblock_t>() <
	 sd.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sd.block_size)),
    [=, &device](auto &bp) {
      return device.dma_read(
	0,
	bp.c_str(),
	sd.block_size
      ).then([=, &bp](auto size)
	     -> BlockSegmentManager::access_ertr::future<block_sm_superblock_t> {
	  if (size < sd.block_size) {
	    // seastar/.../file.hh indicates that a short write here means io
	    // error
	    return crimson::ct_error::input_output_error::make();
	  }
	  bufferlist bl;
	  bl.push_back(bp);
	  block_sm_superblock_t ret;
	  auto bliter = bl.cbegin();
	  ::decode(ret, bliter);
	  return BlockSegmentManager::access_ertr::future<block_sm_superblock_t>(
	    BlockSegmentManager::access_ertr::ready_future_marker{},
	    ret);
      });
    });
}

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
  if (offset < write_pointer || offset % manager.params.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() > manager.params.segment_size)
    return crimson::ct_error::enospc::make();

  return manager.segment_write({id, offset}, bl);
}

Segment::close_ertr::future<> BlockSegmentManager::segment_close(segment_id_t id)
{
  // TODO
  return Segment::close_ertr::now();
}

Segment::write_ertr::future<> BlockSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  logger().debug(
    "segment_write to segment {} at offset {}, physical offset {}, len {}",
    addr.segment,
    addr.offset,
    get_offset(addr),
    bl.length());


  // TODO
  return Segment::write_ertr::now();
}

BlockSegmentManager::~BlockSegmentManager()
{
}

BlockSegmentManager::mount_ret BlockSegmentManager::mount(mount_config_t)
{
  // TODO
  return mount_ertr::now();
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::mkfs(mkfs_config_t config)
{
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    [=](auto &device, auto &stat) {
      return open_device(
	config.path, seastar::open_flags::rw
      ).safe_then([=, &device, &stat](auto p) {
	device = std::move(p.first);
	stat = p.second;
	auto sb = make_superblock(config, stat);
	return write_superblock(device, sb);
      });
    });
}

SegmentManager::open_ertr::future<SegmentRef> BlockSegmentManager::open(
  segment_id_t id)
{
  if (id >= get_num_segments()) {
    logger().error("BlockSegmentManager::open: invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  // TODO
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

  // TODO
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

  if (addr.offset + len > params.segment_size) {
    logger().error(
      "BlockSegmentManager::read: invalid offset {}~{}!",
      addr,
      len);
    return crimson::ct_error::invarg::make();
  }

  // TODO
  return read_ertr::now();
}

}
