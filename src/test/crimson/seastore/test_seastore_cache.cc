// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct CacheTestBlock : LogicalCachedExtent {
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<CacheTestBlock>;

  template <typename... T>
  CacheTestBlock(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new CacheTestBlock(*this));
  };

  Ref duplicate_for_write_concrete() {
    return Ref(new CacheTestBlock(*this));
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

struct cache_test_t : public seastar_test_suite_t {
  segment_manager::EphemeralSegmentManager segment_manager;
  Cache cache;
  paddr_t current;

  cache_test_t()
    : segment_manager(segment_manager::DEFAULT_TEST_EPHEMERAL),
      cache(segment_manager) {}

  seastar::future<std::optional<paddr_t>> submit_transaction(
    TransactionRef t) {
    auto record = cache.try_construct_record(*t);
    if (!record) {
      return seastar::make_ready_future<std::optional<paddr_t>>(
	std::nullopt);
    }

    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(4096 /* TODO */, 0)));
    for (auto &&block : record->extents) {
      bl.append(block.bl);
    }

    ceph_assert((segment_off_t)bl.length() <
		segment_manager.get_segment_size());
    if (current.offset + (segment_off_t)bl.length() >
	segment_manager.get_segment_size())
      current = paddr_t{current.segment + 1, 0};

    auto prev = current;
    current.offset += bl.length();
    return segment_manager.segment_write(
      prev,
      std::move(bl)
    ).safe_then(
      [this, prev, t=std::move(t)] {
	cache.complete_commit(*t, prev);
	return seastar::make_ready_future<std::optional<paddr_t>>(prev);
      },
      crimson::ct_error::all_same_way([](auto e) {
	ASSERT_FALSE("failed to submit");
      })
     );
  }

  seastar::future<> set_up_fut() final {
    return seastar::do_with(
      TransactionRef(new Transaction()),
      [this](auto &transaction) {
	return cache.mkfs(*transaction).safe_then(
	  [this, &transaction] {
	    return submit_transaction(std::move(transaction)).then(
	      [](auto p) {
		ASSERT_TRUE(p);
	      });
	  },
	  crimson::ct_error::all_same_way([](auto e) {
	    ASSERT_FALSE("failed to submit");
	  }));
      });
  }
	    
  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }
};

TEST_F(cache_test_t, basic)
{
  run_async([this] {
  });
}

