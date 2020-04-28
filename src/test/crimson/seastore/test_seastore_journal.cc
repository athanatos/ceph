// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

struct record_validator_t {
  record_t record;
  paddr_t record_final_offset;

  template <typename... T>
  record_validator_t(T&&... record) : record(std::forward<T>(record)...) {}

  void validate(SegmentManager &manager) {
    // TODO: hardcoded to initial record metadata block
    paddr_t addr = make_relative_paddr(4096); 
    for (auto &&block : record.extents) {
      auto test = manager.read(
	record_final_offset.add_relative(addr),
	block.bl.length()).unsafe_get0();
      addr.offset += block.bl.length();
      bufferlist bl;
      bl.push_back(test);
      ASSERT_EQ(bl, block.bl);
    }
  }
};

struct journal_test_t : seastar_test_suite_t, JournalSegmentProvider {
  std::unique_ptr<SegmentManager> segment_manager;
  std::unique_ptr<Journal> journal;

  std::vector<record_validator_t> records;

  journal_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL))
  {
  }

  segment_id_t next = 0;
  get_segment_ret get_segment() final {
    return get_segment_ret(
      get_segment_ertr::ready_future_marker{},
      next++);
  }

  void put_segment(segment_id_t segment) final {
    return;
  }
 
  seastar::future<> set_up_fut() final {
    journal.reset(new Journal(*segment_manager));
    journal->set_segment_provider(this);
    return segment_manager->init(
    ).safe_then([this] {
      return journal->open_for_write();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      }));
  }

  template <typename T>
  auto replay(T &&f) {
    return journal->close(
    ).safe_then([this, f=std::move(f)]() mutable {
      journal.reset(new Journal(*segment_manager));
      journal->set_segment_provider(this);
      return journal->replay(std::forward<T>(std::move(f)));
    }).safe_then([this] {
      return journal->open_for_write();
    });
  }

  auto replay_and_check() {
    auto record_iter = records.begin();
    if (record_iter == records.end()) {
      replay([](...) {
	EXPECT_FALSE("No Deltas");
	return Journal::replay_ertr::now();
      }).unsafe_get0();
    } else {
      std::vector<delta_info_t>::iterator delta_iter = record_iter->record.deltas.begin();
      replay(
	[this, record_iter, delta_iter](auto base, const auto &di) mutable {
	  if (record_iter == records.end()) {
	    EXPECT_FALSE("No Deltas Left");
	  }
	  while (delta_iter == record_iter->record.deltas.end()) {
	    ++record_iter;
	    if (record_iter == records.end()) {
	      EXPECT_FALSE("No Deltas Left");
	    }
	    delta_iter = record_iter->record.deltas.begin();
	  }
	  EXPECT_EQ(di, *delta_iter++);
	  EXPECT_EQ(base, record_iter->record_final_offset);
	  return Journal::replay_ertr::now();
	}).unsafe_get0();
    }
  }

  template <typename... T>
  auto submit_record(T&&... record) {
    records.push_back(std::forward<T>(record)...);
  }

  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }
};

TEST_F(journal_test_t, basic)
{
 run_async([this] {
 });
}

