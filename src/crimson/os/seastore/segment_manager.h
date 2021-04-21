// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class Segment : public boost::intrusive_ref_counter<
  Segment,
  boost::thread_unsafe_counter>{
public:

  enum class segment_state_t : uint8_t {
    EMPTY = 0,
    OPEN = 1,
    CLOSED = 2
  };

  /**
   * get_segment_id
   */
  virtual segment_id_t get_segment_id() const = 0;

  /**
   * min next write location
   */
  virtual segment_off_t get_write_ptr() const = 0;

  /**
   * max capacity
   */
  virtual segment_off_t get_write_capacity() const = 0;

  /**
   * close
   *
   * Closes segment for writes.  Won't complete until
   * outstanding writes to this segment are complete.
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual close_ertr::future<> close() = 0;


  /**
   * write
   *
   * @param offset offset of write, must be aligned to <> and >= write pointer, advances
   *               write pointer
   * @param bl     buffer to write, will be padded if not aligned
  */
  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >;
  virtual write_ertr::future<> write(
    segment_off_t offset, ceph::bufferlist bl) = 0;

  virtual ~Segment() {}
};
using SegmentRef = boost::intrusive_ptr<Segment>;

constexpr size_t PADDR_SIZE = sizeof(paddr_t);

class SegmentManager {
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
  virtual mount_ret mount(const mount_config_t&) = 0;

  struct mkfs_config_t {
    std::string path;
    size_t segment_size = 0;
    size_t total_size = 0;
    seastore_meta_t meta;
  };
  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(mkfs_config_t) = 0;


  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual open_ertr::future<SegmentRef> open(segment_id_t id) = 0;

  using release_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual release_ertr::future<> release(segment_id_t id) = 0;

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) = 0;
  read_ertr::future<ceph::bufferptr> read(
    paddr_t addr,
    size_t len) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref).safe_then(
      [ptrref=std::move(ptrref)]() mutable {
	return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
      });
  }

  /* Methods for discovering device geometry, segmentid set, etc */
  virtual size_t get_size() const = 0;
  virtual segment_off_t get_block_size() const = 0;
  virtual segment_off_t get_segment_size() const = 0;
  virtual segment_id_t get_num_segments() const {
    ceph_assert(get_size() % get_segment_size() == 0);
    return ((segment_id_t)(get_size() / get_segment_size()));
  }
  virtual const seastore_meta_t &get_meta() const = 0;

  virtual ~SegmentManager() {}
};
using SegmentManagerRef = std::unique_ptr<SegmentManager>;

}
