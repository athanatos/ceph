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

void SegmentCleaner::update_journal_tail_target(journal_seq_t target)
{
  logger().debug(
    "{}: {}",
    __func__,
    target);
  assert(journal_tail_target == journal_seq_t() || target >= journal_tail_target);
  if (journal_tail_target == journal_seq_t() || target > journal_tail_target) {
    journal_tail_target = target;
  }
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  if (journal_tail_committed == journal_seq_t() ||
      committed > journal_tail_committed) {
    logger().debug(
      "{}: update journal_tail_committed {}",
      __func__,
      committed);
    journal_tail_committed = committed;
    }
  if (journal_tail_target == journal_seq_t() ||
      committed > journal_tail_target) {
    logger().debug(
      "{}: update journal_tail_target {}",
      __func__,
      committed);
    journal_tail_target = committed;
  }
}

void SegmentCleaner::put_segment(segment_id_t segment)
{
  assert(segment < segments.size());
  segments[segment].state = Segment::segment_state_t::CLOSED;
}

SegmentCleaner::do_immediate_work_ret SegmentCleaner::do_immediate_work(
  Transaction &t)
{
  auto next_target = get_dirty_tail_limit();
  logger().debug(
    "{}: journal_tail_target={} get_dirty_tail_limit()={}",
    __func__,
    journal_tail_target,
    next_target);
  if (journal_tail_target > next_target) {
    return do_immediate_work_ertr::now();
  }

  return ecb->get_next_dirty_extents(
    get_dirty_tail_limit()
  ).then([=, &t](auto dirty_list) {
    if (dirty_list.empty()) {
      return do_immediate_work_ertr::now();
    } else {
      update_journal_tail_target(dirty_list.front()->get_dirty_from());
    }
    return seastar::do_with(
      std::move(dirty_list),
      [this, &t](auto &dirty_list) {
	return crimson::do_for_each(
	  dirty_list.begin(),
	  dirty_list.end(),
	  [this, &t](auto &e) {
	    logger().debug(
	      "SegmentCleaner::do_immediate_work cleaning {}",
	      *e);
	    return ecb->rewrite_extent(
	      t,
	      e);
	  });
      });
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
