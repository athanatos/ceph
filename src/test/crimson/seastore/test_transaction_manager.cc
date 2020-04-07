// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

struct transaction_manager_test_t : public ::testing::Test {
  std::unique_ptr<SegmentManager> segment_manager;
  Cache cache;
  TransactionManager tm;

  transaction_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      cache(*segment_manager),
      tm(*segment_manager, cache) {}
};

TEST_F(transaction_manager_test_t, basic)
{
  // TODO
}

