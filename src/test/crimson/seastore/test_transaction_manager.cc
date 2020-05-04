// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

struct TestBlock : LogicalCachedExtent {
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlock>;

  template <typename... T>
  TestBlock(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new TestBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    return ceph::bufferlist();
  }

  void apply_delta(paddr_t delta_base, ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  void set_lba_root(btree_lba_root_t lba_root);
};

struct transaction_manager_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Journal journal;
  Cache cache;
  LBAManagerRef lba_manager;
  TransactionManager tm;

  transaction_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(
	lba_manager::create_lba_manager(*segment_manager, cache)),
      tm(*segment_manager, journal, cache, *lba_manager) {}

  seastar::future<> set_up_fut() final {
    return segment_manager->init().safe_then([this] {
      return tm.mkfs();
    }).safe_then([this] {
      return tm.mount();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return tm.close(
    ).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }
};

TEST_F(transaction_manager_test_t, basic)
{
  constexpr laddr_t ADDR = 0xFF * (TestBlock::SIZE);
  run_async([this] {
    {
      auto t = tm.create_transaction();
      auto extent = tm.alloc_extent<TestBlock>(
	*t,
	ADDR,
	TestBlock::SIZE).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm.create_transaction();
      auto extent = tm.read_extents<TestBlock>(
	*t,
	ADDR,
	TestBlock::SIZE).unsafe_get0();
    }
  });
}
