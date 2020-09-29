// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "crimson/common/layout.h"

#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore::segment_manager::block {

/**
 * SegmentStateTracker
 *
 * Tracks lifecycle state of each segment using space at the beginning
 * of the drive.
 */
class SegmentStateTracker {
  using segment_state_t = Segment::segment_state_t;
  struct segment_state_block_t {
    using L = absl::container_internal::Layout<uint8_t>;
    static constexpr size_t SIZE = 4<<10;
    static constexpr size_t CAPACITY = SIZE;
    static constexpr L layout{CAPACITY};

    bufferptr bptr;
    segment_state_block_t() : bptr(CAPACITY) {}

    segment_state_t get(size_t offset) {
      return static_cast<segment_state_t>(
	layout.template Pointer<0>(
	  bptr.c_str())[offset]);
    }

    void set(size_t offset, segment_state_t state) {
      layout.template Pointer<0>(bptr.c_str())[offset] =
	static_cast<uint8_t>(state);
    }
  };
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
  friend class BlockSegment;
  using segment_state_t = Segment::segment_state_t;

  struct block_config_t {
    std::string path;
    size_t size = 0;
    size_t block_size = 0;
    size_t segment_size = 0;
  };
  const block_config_t config;

  size_t get_offset(paddr_t addr) {
    return (addr.segment * config.segment_size) + addr.offset;
  }

  std::vector<segment_state_t> segment_state;

  char *buffer = nullptr;

  Segment::close_ertr::future<> segment_close(segment_id_t id);

public:
  BlockSegmentManager() = default;
  ~BlockSegmentManager();

  open_ertr::future<SegmentRef> open(segment_id_t id) final;

  release_ertr::future<> release(segment_id_t id) final;

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final;

  size_t get_size() const final {
    return config.size;
  }
  segment_off_t get_block_size() const {
    return config.block_size;
  }
  segment_off_t get_segment_size() const {
    return config.segment_size;
  }

  void reopen();

  // public so tests can bypass segment interface when simpler
  Segment::write_ertr::future<> segment_write(
    paddr_t addr,
    ceph::bufferlist bl,
    bool ignore_check=false);
};

}
