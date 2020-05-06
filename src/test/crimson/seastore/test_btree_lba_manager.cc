// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::lba_manager;
using namespace crimson::os::seastore::lba_manager::btree;

struct btree_lba_manager_test :
  public seastar_test_suite_t, JournalSegmentProvider {
  SegmentManagerRef segment_manager;
  Journal journal;
  Cache cache;
  BtreeLBAManagerRef lba_manager;

  const size_t block_size;

  btree_lba_manager_test()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(new BtreeLBAManager(*segment_manager, cache)),
      block_size(segment_manager->get_block_size())
  {
    journal.set_segment_provider(this);
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

  auto submit_transaction(TransactionRef t)
  {
    auto record = cache.try_construct_record(*t);
    if (!record) {
      ceph_assert(0 == "cannot fail");
    }
    
    return journal.submit_record(std::move(*record)).safe_then(
      [this, t=std::move(t)](paddr_t addr) {
	cache.complete_commit(*t, addr);
      },
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      }));
  }

  seastar::future<> set_up_fut() final {
    return segment_manager->init(
    ).safe_then([this] {
      return journal.open_for_write();
    }).safe_then([this] {
      return seastar::do_with(
	lba_manager->create_transaction(),
	[this](auto &transaction) {
	  return cache.mkfs(*transaction
	  ).safe_then([this, &transaction] {
	    return lba_manager->mkfs(*transaction);
	  }).safe_then([this, &transaction] {
	    return submit_transaction(std::move(transaction));
	  });
	});
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ceph_assert(0 == "error");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return cache.close(
    ).safe_then([this] {
      return journal.close();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }

  
  struct test_extent_t {
    paddr_t addr;
    size_t len = 0;
  };
  std::map<laddr_t, test_extent_t> test_lba_mappings;
  auto get_overlap(laddr_t addr, size_t len) {
    auto bottom = test_lba_mappings.upper_bound(addr);
    if (bottom != test_lba_mappings.begin())
      --bottom;
    if (bottom != test_lba_mappings.end() &&
	bottom->first + bottom->second.len <= addr)
      ++bottom;
    
    auto top = test_lba_mappings.upper_bound(addr + len);
    return std::make_pair(
      bottom,
      top
    );
  }

  auto alloc_mapping(Transaction &t, laddr_t hint, size_t len, paddr_t paddr) {
    auto ret = lba_manager->alloc_extent(t, hint, len, paddr).unsafe_get0();
    EXPECT_EQ(len, ret->get_length());
    auto [b, e] = get_overlap(ret->get_laddr(), len);
    EXPECT_EQ(b, e);
    test_lba_mappings.emplace(
      std::make_pair(
	ret->get_laddr(),
	test_extent_t{
	  ret->get_paddr(),
	  ret->get_length()
        }
      ));
    return ret;
  }

  auto set_mapping(Transaction &t, laddr_t addr, size_t len, paddr_t paddr) {
    auto [b, e] = get_overlap(addr, len);
    EXPECT_EQ(b, e);

    auto ret = lba_manager->set_extent(t, addr, len, paddr).unsafe_get0();
    EXPECT_EQ(addr, ret->get_laddr());
    EXPECT_EQ(len, ret->get_length());
    EXPECT_EQ(paddr, ret->get_paddr());
    test_lba_mappings.emplace(
      std::make_pair(
	ret->get_laddr(),
	test_extent_t{
	  ret->get_paddr(),
	  ret->get_length()
        }
      ));
    return ret;
  }

  auto remove_mapping(Transaction &t, laddr_t addr, size_t len) {
  }
};

TEST_F(btree_lba_manager_test, basic)
{
  run_async([this] {
    auto t = lba_manager->create_transaction();
    auto ret = alloc_mapping(*t, 0, block_size, P_ADDR_MIN);
    submit_transaction(std::move(t)).get0();
  });
}

