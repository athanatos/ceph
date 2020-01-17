// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

namespace transaction_manager_detail {
class Journal {
  SegmentManager &segment_manager;

  segment_id_t current_journal_segment_id = NULL_SEG_ID;
  SegmentRef current_journal_segment;

  segment_id_t next_journal_segment_id = NULL_SEG_ID;
  journal_seq_t current_journal_seq = 0;

  segment_id_t get_next_journal_segment_id() {
    // TODO
    return 0;
  }

  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<> roll_journal_segment();

  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<> initialize_segment(
    Segment &segment,
    segment_id_t next_journal_segment);

  
public:
  Journal(SegmentManager &segment_manager) : segment_manager(segment_manager) {}

  using init_ertr = crimson::errorator <
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> init();
};

Journal::init_ertr::future<> Journal::init()
{
  next_journal_segment_id = get_next_journal_segment_id();
  return roll_journal_segment();
}

Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment,
  segment_id_t next_journal_segment)
{
  // write out header
  return initialize_segment_ertr::now();
}

Journal::roll_journal_segment_ertr::future<>
Journal::roll_journal_segment()
{
  return current_journal_segment->close().handle_error(
    //roll_journal_segment_ertr::pass_further{},
    roll_journal_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([this](auto &&e) {
      logger().error(
	"error {} in close segment {}",
	e,
	current_journal_segment_id);
      ceph_assert(0 == "error in close");
      return;
    })
  ).safe_then([this] {
    return segment_manager.open(next_journal_segment_id);
  }).handle_error(
    roll_journal_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([this](auto &&e) {
      logger().error(
	"error {} in close segment {}",
	e,
	current_journal_segment_id);
      ceph_assert(0 == "error in close");
      return;
    })
  ).safe_then([this](auto segment) {
    current_journal_segment = segment;
    current_journal_segment_id = next_journal_segment_id;
    next_journal_segment_id = get_next_journal_segment_id();
    return get_next_journal_segment_id();
  }).safe_then([this](auto segment_id) {
    next_journal_segment_id = segment_id;
    return initialize_segment(
      *current_journal_segment, next_journal_segment_id);
  });
}

}

Transaction::Transaction(paddr_t start) : start(start) {}

TransactionManager::TransactionManager(SegmentManager &segment_manager)
  : journal(new transaction_manager_detail::Journal(segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->init();
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

