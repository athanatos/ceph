// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

struct transaction_manager_test_t : public seastar_test_case_t {
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
};

TEST_F(transaction_manager_test_t, basic)
{
  // TODO
}

