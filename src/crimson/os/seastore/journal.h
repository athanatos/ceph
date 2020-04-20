// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

using journal_seq_t = uint64_t;
static constexpr journal_seq_t NO_DELTAS =
  std::numeric_limits<journal_seq_t>::max();

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_seq
 * 2) Scan forward from committed_journal_lb to find the most recent
 *    journal_commit_lb record
 * 3) Replay starting at the most recent found journal_commit_lb record
 */
struct segment_header_t {
  segment_seq_t journal_segment_seq;
  segment_id_t physical_segment_id; // debugging

  paddr_t journal_replay_lb;

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.journal_segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_replay_lb, p);
    DENC_FINISH(p);
  }
};

struct record_header_t {
  // Fixed portion
  segment_off_t mdlength;       // block aligned
  segment_off_t dlength;        // block aligned
  journal_seq_t seq;            // current journal seqid
  checksum_t    full_checksum;  // checksum for full record
  size_t deltas;                // number of deltas
  size_t extents;               // number of extents

  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.seq, p);
    denc(v.full_checksum, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }
};

/**
 * Callback interface for managing available segments
 */
class JournalSegmentProvider {
public:
  using get_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_segment_ret = get_segment_ertr::future<segment_id_t>;
  virtual get_segment_ret get_segment() = 0;
  
  /* TODO: we'll want to use this to propogate information about segment contents */
  virtual void put_segment(segment_id_t segment) = 0;

  virtual ~JournalSegmentProvider() {}
};

class Journal {
  const segment_off_t block_size;
  const segment_off_t max_record_length;

  JournalSegmentProvider *segment_provider = nullptr;
  SegmentManager &segment_manager;

  paddr_t current_replay_point;

  segment_seq_t current_journal_segment_seq = 0;
  
  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;

  segment_id_t next_journal_segment_seq = NULL_SEG_ID;
  journal_seq_t current_journal_seq = 0;

  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<> initialize_segment(
    Segment &segment);

  ceph::bufferlist encode_record(
    segment_off_t mdlength,
    segment_off_t dlength,
    record_t &&record);

  using write_record_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  write_record_ertr::future<> write_record(
    segment_off_t mdlength,
    segment_off_t dlength,
    record_t &&record);
  
  bool needs_roll(segment_off_t length) const;

  paddr_t next_block_addr() const;

  using find_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using find_replay_segments_fut = 
    find_replay_segments_ertr::future<std::vector<paddr_t>>;
  find_replay_segments_fut find_replay_segments();

public:
  Journal(SegmentManager &segment_manager);

  void set_segment_provider(JournalSegmentProvider *provider) {
    segment_provider = provider;
  }

  /**
   * Return <mdlength, dlength> pair denoting length of
   * metadata and blocks respectively.
   */
  std::pair<segment_off_t, segment_off_t> get_encoded_record_length(
    const record_t &record) const;

  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<> roll_journal_segment();

  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> open_for_write();

  /**
   * close
   *
   * TODO: should probably flush and disallow further writes
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  close_ertr::future<> close() { return close_ertr::now(); }

  /**
   * write_record
   *
   * @param write record and returns offset of first block
   */
  using submit_record_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error
    >;
  using submit_record_ret = submit_record_ertr::future<paddr_t>;
  submit_record_ret submit_record(record_t &&record) {
    auto [mdlength, dlength] = get_encoded_record_length(record);
    auto total = mdlength + dlength;
    if (total > max_record_length) {
      return crimson::ct_error::erange::make();
    }
    auto ret = next_block_addr();
    return (needs_roll(total)
      ? roll_journal_segment()
      : roll_journal_segment_ertr::now()).safe_then(
	[this, mdlength, dlength, record=std::move(record)]() mutable {
	  return write_record(mdlength, dlength, std::move(record));
	}).safe_then([ret] { return ret; });
  }

  using replay_ertr = SegmentManager::read_ertr;
  using replay_ret = replay_ertr::future<>;
  using delta_handler_t = std::function<replay_ret(paddr_t record_start, const delta_info_t&)>;
  replay_ret replay(delta_handler_t &&);

private:
  using read_record_metadata_ertr = replay_ertr;
  using read_record_metadata_ret = read_record_metadata_ertr::future<
    std::optional<std::pair<record_header_t, bufferlist>>
    >;
  read_record_metadata_ret read_record_metadata(
    paddr_t start);

  std::optional<std::vector<delta_info_t>> try_decode_deltas(
    record_header_t header,
    bufferlist &bl);

  replay_ertr::future<>
  replay_segment(
    paddr_t start,
    delta_handler_t &delta_handler);
};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)

