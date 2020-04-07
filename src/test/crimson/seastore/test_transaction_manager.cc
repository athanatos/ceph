// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/seastore/cache.h"
#include "crimson/seastore/transaction_manager.h"
#include "crimson/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::seastore;

struct transaction_manager_test_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Cache cache;
  TransactionManager tm

  transaction_manager_test_t()
    : segment_manager(create_ephemeral(DEFAULT_TEST_EPHEMERAL)),
      tm(*segment_manager, cache) {}
};

TEST_F(transaction_manager_test_t, basic)
{
  // TODO
}
