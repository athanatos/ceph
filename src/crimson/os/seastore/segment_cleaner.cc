// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/segment_cleaner.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

SegmentCleaner::get_segment_ret SegmentCleaner::get_segment()
{
  for (size_t i = 0; i < segments.size(); ++i) {
    if (segments[i].is_empty()) {
      segments[i].state = Segment::segment_state_t::OPEN;
      return get_segment_ret(
	get_segment_ertr::ready_future_marker{},
	i);
    }
  }
  assert(0 == "out of space handling todo");
  return get_segment_ret(
    get_segment_ertr::ready_future_marker{},
    0);
}

void SegmentCleaner::put_segment(segment_id_t segment)
{
  assert(segment < segments.size());
  segments[segment].state = Segment::segment_state_t::CLOSED;
}

SegmentCleaner::do_immediate_work_ret SegmentCleaner::do_immediate_work(
  Transaction &t)
{
  // TODO: link in the set_segment_seq and do_immediate_work
  auto last_dirty = ecb->get_next_dirty_extent();
  if (!last_dirty) {
    return do_immediate_work_ertr::now();
  }

  auto dirty_seq = last_dirty->get_dirty_from();
  assert(journal_tail_target <= dirty_seq);

  journal_tail_target = dirty_seq;
  if (get_current_journal_score() < get_journal_score_limit()) {
    return do_immediate_work_ertr::now();
  }

  return ecb->rewrite_extent(
    t,
    last_dirty
  ).safe_then([this, &t] {
    return do_immediate_work(t);
  });
}

SegmentCleaner::do_deferred_work_ret SegmentCleaner::do_deferred_work(
  Transaction &t)
{
  return do_deferred_work_ret(
    do_deferred_work_ertr::ready_future_marker{},
    ceph::timespan());
}

}
