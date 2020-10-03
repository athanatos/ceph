// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "crimson/common/layout.h"

#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore::segment_manager::block {

using write_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using read_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;

/**
 * SegmentStateTracker
 *
 * Tracks lifecycle state of each segment using space at the beginning
 * of the drive.
 */
class SegmentStateTracker {
  using segment_state_t = Segment::segment_state_t;

  bufferptr bptr;

  using L = absl::container_internal::Layout<uint8_t>;
  const L layout;

public:
  static size_t get_raw_size(size_t segments, size_t block_size) {
    return p2roundup(segments, block_size);
  }

  SegmentStateTracker(size_t segments, size_t block_size)
    : bptr(ceph::buffer::create_page_aligned(
	     get_raw_size(segments, block_size))),
      layout(bptr.length())
  {}

  size_t get_size() const {
    return bptr.length();
  }

  size_t get_capacity() const {
    return bptr.length();
  }

  segment_state_t get(size_t offset) {
    assert(offset < get_capacity());
    return static_cast<segment_state_t>(
      layout.template Pointer<0>(
	bptr.c_str())[offset]);
  }

  void set(size_t offset, segment_state_t state) {
    assert(offset < get_capacity());
    layout.template Pointer<0>(bptr.c_str())[offset] =
      static_cast<uint8_t>(state);
  }

  write_ertr::future<> write_out(
    seastar::file &device,
    uint64_t offset);

  read_ertr::future<> read_in(
    seastar::file &device,
    uint64_t offset);
};

class BlockSegmentManager;
class BlockSegment final : public Segment {
  friend class BlockSegmentManager;
  BlockSegmentManager &manager;
  const segment_id_t id;
  segment_off_t write_pointer = 0;
public:
  BlockSegment(BlockSegmentManager &manager, segment_id_t id);

  segment_id_t get_segment_id() const final { return id; }
  segment_off_t get_write_capacity() const final;
  segment_off_t get_write_ptr() const final { return write_pointer; }
  close_ertr::future<> close() final;
  write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) final;

  ~BlockSegment() {}
};

class BlockSegmentManager final : public SegmentManager {
public:
  using access_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;


  struct mount_config_t {
    std::string path;
  };
  using mount_ertr = access_ertr;
  using mount_ret = access_ertr::future<>;
  mount_ret mount(mount_config_t);

  struct mkfs_config_t {
    std::string path;
    size_t segment_size = 0;
  };
  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  static mkfs_ret mkfs(mkfs_config_t);

  struct block_params_t {
    std::string path;

    // superblock/mkfs option
    size_t segment_size = 0;

    // inferred from device geometry
    size_t size = 0;
    size_t block_size = 0;
  };

  BlockSegmentManager() = default;
  ~BlockSegmentManager();

  open_ertr::future<SegmentRef> open(segment_id_t id) final;

  release_ertr::future<> release(segment_id_t id) final;

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final;

  size_t get_size() const final {
    return params.size;
  }
  segment_off_t get_block_size() const {
    return params.block_size;
  }
  segment_off_t get_segment_size() const {
    return params.segment_size;
  }

  void reopen();

  // public so tests can bypass segment interface when simpler
  Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);

private:
  friend class BlockSegment;
  using segment_state_t = Segment::segment_state_t;

  block_params_t params;
  seastar::file device;

  size_t get_offset(paddr_t addr) {
    return (addr.segment * params.segment_size) + addr.offset;
  }

  std::vector<segment_state_t> segment_state;

  char *buffer = nullptr;

  Segment::close_ertr::future<> segment_close(segment_id_t id);
};

}

