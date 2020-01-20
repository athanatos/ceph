// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }

}

/**
 * Segment header
 */
struct segment_header_t {
  crimson::os::seastore::segment_id_t next_segment;
  crimson::os::seastore::journal_seq_t journal_lb;

  DENC(segment_header_t, v, p) {
    denc(v.next_segment, p);
    denc(v.journal_lb, p);
  }
};
WRITE_CLASS_DENC(segment_header_t)

namespace crimson::os::seastore {
namespace transaction_manager_detail {

class Journal {
  const segment_off_t block_size;
  
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
  Journal(segment_off_t block_size) : block_size(block_size) {}

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
  segment_off_t reserve(segment_off_t size);
};

Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment,
  segment_id_t next_journal_segment)
{
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;
  auto header = segment_header_t{next_journal_segment, current_journal_seq};
  ::encode(header, bl);
  reserved_to = block_size;
  written_to = block_size;
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

segment_off_t Journal::reserve(segment_off_t size)
{
  ceph_assert(size % block_size == 0);
  if (reserved_to + size >= block_size) {
    return NULL_SEG_OFF;
  } else {
    auto offset = reserved_to;
    reserved_to += size;
    return offset;
  }
}

}

Transaction::Transaction(paddr_t start) : start(start) {}

TransactionManager::TransactionManager(SegmentManager &segment_manager)
  : journal(new transaction_manager_detail::Journal(
	      segment_manager.get_block_size()))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->init_write(SegmentRef(), 0 /* TODO */);
}

TransactionManager::read_ertr::future<> TransactionManager::add_delta(
  TransactionRef &trans,
  paddr_t paddr,
  laddr_t laddr,
  ceph::bufferlist delta,
  ceph::bufferlist bl)
{
  return read_ertr::now();
}

paddr_t TransactionManager::add_block(
  TransactionRef &trans,
  laddr_t laddr,
  ceph::bufferlist bl)
{
  return {0, 0};
}

TransactionManager::~TransactionManager() {}

}

