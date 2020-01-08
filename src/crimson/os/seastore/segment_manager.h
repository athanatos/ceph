// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

namespace crimson::os::seastore {

class Segment : public boost::intrusive_ref_counter<
  Segment,
  boost::thread_unsafe_counter>{
};
using SegmentRef = boost::intrusive_ptr<Segment>;
                                        

class SegmentManager {
public:
  open_ertr<Segment> open(segment_id_t id) Opens a segment for write.

  read_ertr<bufferlist> read(segment_id_t id, segment_off_t offset, size_t len);

  write_ertr<> release(segment_id_t id);


  /* Methods for discovering device geometry, segmentid set, etc */
};

};
