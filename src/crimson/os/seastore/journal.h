// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {
class SegmentManager;
class Segment;
using SegmentRef = boost::intrusive_ptr<Segment>;

class Journal {
public:
  using journal_seq_t = uint64_t;
  static constexpr journal_seq_t NO_DELTAS = std::numeric_limits<journal_seq_t>::max();

  using journal_segment_seq_t = uint64_t;
  static constexpr journal_segment_seq_t NO_JOURNAL =
    std::numeric_limits<journal_segment_seq_t>::max();

private:
  SegmentManager &segment_manager;

  paddr_t current_replay_point;

  journal_segment_seq_t current_journal_segment_id = 0;
  
  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;
  segment_off_t reserved_to = 0;

  segment_id_t next_journal_segment_id = NULL_SEG_ID;
  journal_seq_t current_journal_seq = 0;

  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<> initialize_segment(
    Segment &segment,
    segment_id_t next_journal_segment);

  
public:
  Journal(
    SegmentManager &segment_manager)
    : segment_manager(segment_manager) {}

  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<SegmentRef> roll_journal_segment(
    SegmentRef next_journal_segment,
    segment_id_t next_next_id);

  using init_ertr = crimson::errorator <
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> init_write(
    SegmentRef initial_journal_segment,
    segment_id_t next_next_id);

  /* Reserves space for size bytes (must be block aligned), returns NULL_SEG
   * if journal doesn't have enough space */
  std::pair<segment_off_t, SegmentRef> reserve(segment_off_t size);

};

}
