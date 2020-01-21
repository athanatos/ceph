// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/journal.h"

#include "include/denc.h"
#include "include/intarith.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace {
using namespace crimson;
using namespace crimson::os::seastore;

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_id
 * 2) Scan forward from committed_journal_lb to find the most recent
 *    journal_commit_lb record
 * 3) Replay starting at the most recent found journal_commit_lb record
 */
struct segment_header_t {
  Journal::journal_segment_seq_t journal_segment_id;
  segment_id_t physical_segment_id; // debugging

  paddr_t journal_replay_lb;

  DENC(segment_header_t, v, p) {
    denc(v.journal_segment_id, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_replay_lb, p);
  }
};


/* Could be modified to be omitted for logical blocks */
struct extent_info_t {
  // offset inferred from position in list
  segment_off_t length;
  laddr_t laddr; // Encodes type for non-logical, debugging info for
  // logical (lba tree already has a secondary lookup
  // for checking logical block liveness)
  // Note, we could omit this for logical blocks, replace laddr
  // with another segment_off_t for offset, and stash the type tag in the
  // low bits.
};

struct record_header_t {
  // Fixed portion
  segment_off_t length;         // block aligned
  Journal::journal_seq_t seq;            // journal sequence for
  segment_off_t tail;           // overflow for long record metadata
  checksum_t    full_checksum;  // checksum for full record
};

struct record_t : record_header_t {
  std::vector<ceph::bufferlist> extents; // block aligned, offset and length
  ceph::bufferlist delta;
  std::vector<extent_info_t> block_info; // information on each extent
};

}
WRITE_CLASS_DENC(segment_header_t)

namespace crimson::os::seastore {

Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment,
  segment_id_t next_journal_segment)
{
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;
  auto header = segment_header_t{
    current_journal_segment_id++,
    segment.get_segment_id(),
    current_replay_point};
  ::encode(header, bl);
  reserved_to = segment_manager.get_block_size();
  written_to = segment_manager.get_block_size();
  return segment.write(0, bl).handle_error(
    init_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); }));
}

Journal::init_ertr::future<> Journal::init_write(
  SegmentRef initial_journal_segment,
  segment_id_t next_next_id)
{
  return roll_journal_segment(initial_journal_segment, next_next_id).safe_then(
    [](auto) { return; });
}

Journal::roll_journal_segment_ertr::future<SegmentRef>
Journal::roll_journal_segment(
  SegmentRef next_journal_segment,
  segment_id_t next_next_id)
{
  auto old_segment = current_journal_segment;
  current_journal_segment = next_journal_segment;
  next_journal_segment_id = next_next_id;
  written_to = 0;
  reserved_to = 0;
  return initialize_segment(
    *current_journal_segment,
    next_journal_segment_id).safe_then([old_segment] {
      return old_segment;
    });
}

std::pair<segment_off_t, SegmentRef> Journal::reserve(segment_off_t size)
{
  ceph_assert(size % segment_manager.get_block_size() == 0);
  if (reserved_to + size >= current_journal_segment->get_write_capacity()) {
    return std::make_pair(NULL_SEG_OFF, nullptr);
  } else {
    auto offset = reserved_to;
    reserved_to += size;
    return std::make_pair(
      offset, current_journal_segment);
  }
}

}
