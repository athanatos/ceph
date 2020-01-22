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

/**
 * Callback interface for managing available segments
 */
class JournalSegmentProvider {
public:
  using get_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual get_segment_ertr::future<segment_id_t> get_segment() = 0;
  
  /* TODO: we'll want to use this to propogate information about segment contents */
  virtual void put_segment(segment_id_t segment) = 0;

  virtual ~JournalSegmentProvider() {}
};

class Journal {
public:
  using journal_segment_seq_t = uint64_t;
  static constexpr journal_segment_seq_t NO_JOURNAL =
    std::numeric_limits<journal_segment_seq_t>::max();

private:
  const segment_off_t block_size;
  const segment_off_t max_record_length;

  JournalSegmentProvider &segment_provider;
  SegmentManager &segment_manager;

  paddr_t current_replay_point;

  journal_segment_seq_t current_journal_segment_id = 0;
  
  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;

  segment_id_t next_journal_segment_id = NULL_SEG_ID;
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
  
  std::pair<segment_off_t, segment_off_t> get_encoded_record_length(
    const record_t &record) const;

  bool needs_roll(segment_off_t length) const;

  paddr_t next_block_addr() const;

public:
  Journal(
    JournalSegmentProvider &segment_provider,
    SegmentManager &segment_manager);

  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<> roll_journal_segment();

  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> open_for_write();

  using write_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error
    >;
  template <typename F>
  write_ertr::future<> write_with_offset(
    record_t &&record,
    F &&fixup) {
    auto [mdlength, dlength] = get_encoded_record_length(record);
    auto total = mdlength + dlength;
    if (total > max_record_length) {
      return crimson::ct_error::erange::make();
    }
    return (needs_roll(total) ? roll_journal_segment() :
	    roll_journal_segment_ertr::now()
    ).safe_then([this,
		 fixup=std::forward<F>(fixup),
		 record=std::move(record),
		 mdlength,
		 dlength,
		 total]() mutable {
      fixup(written_to + block_size, record);
      return write_record(mdlength, dlength, std::move(record));
    });
  }
};

}
