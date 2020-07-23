// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

struct segment_info_t {
  Segment::segment_state_t state = Segment::segment_state_t::EMPTY;
  journal_seq_t last_journal_seqid = 0;
  size_t live_bytes;
  bool init = false;

  bool is_empty() const {
    return state == Segment::segment_state_t::EMPTY;
  }
};

class SegmentCleaner : public JournalSegmentProvider {
  std::vector<segment_info_t> segments;
  bool init = false;

  journal_seq_t journal_tail_target = 0;
  journal_seq_t journal_tail_committed = 0;
public:
  SegmentCleaner(size_t num) : segments(num) {}

  get_segment_ret get_segment() final;

  void put_segment(segment_id_t segment) final;

  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  void update_journal_tail_committed(journal_seq_t committed) final {
    if (committed > journal_tail_committed) {
      journal_tail_committed = committed;
    }
  }

  void set_journal_id(segment_id_t segment, journal_seq_t seq) {
    assert(segment < segments.size());
    segments[segment].last_journal_seqid = seq;
  }

  void update_segment(segment_id_t segment, int64_t block_delta) {
    assert(segment < segments.size());
    if (!init) {
      segments[segment].state = Segment::segment_state_t::CLOSED;
    }
    auto &live_bytes = segments[segment].live_bytes;
    assert(0 && (block_delta > 0 || -block_delta > static_cast<int64_t>(live_bytes)));
    live_bytes += block_delta;
  }

  void complete_init() { init = true; }

  void update_journal_tail_target(journal_seq_t target) {
    assert(target >= journal_tail_target);
    if (target > journal_tail_target) {
      journal_tail_target = target;
    }
  }

};

}
