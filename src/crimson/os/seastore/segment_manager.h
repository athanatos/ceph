// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

using segment_id_t = uint32_t;
using segment_off_t = uint32_t;
struct paddr_t {
  segment_id_t segment;
  segment_off_t offset;
};

class Segment : public boost::intrusive_ref_counter<
  Segment,
  boost::thread_unsafe_counter>{
public:

  /**
   * close
   *
   * Closes segment for writes.
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
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
  virtual write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) = 0;

  virtual ~Segment() {}
};
using SegmentRef = boost::intrusive_ptr<Segment>;

constexpr size_t PADDR_SIZE = sizeof(paddr_t);

class SegmentManager {
public:
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
  virtual read_ertr::future<ceph::bufferlist> read(
    paddr_t addr,
    size_t len) = 0;

  /* Methods for discovering device geometry, segmentid set, etc */
  virtual ~SegmentManager() {}
};
using SegmentManagerRef = std::unique_ptr<SegmentManager>;

namespace segment_manager {

struct ephemeral_config_t {
  size_t size;
  size_t block_size;
  size_t segment_size;
};
std::ostream &operator<<(std::ostream &, const ephemeral_config_t &);
SegmentManagerRef create_ephemeral(ephemeral_config_t config);

}

};
