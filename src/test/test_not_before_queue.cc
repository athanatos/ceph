// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/not_before_queue.h"
#include "gtest/gtest.h"

struct test_value_t {
  utime_t not_before;
  unsigned ordering_value = 0;
};

utime_t project_not_before(const test_value_t &v) {
  return v.not_before;
}

bool operator<(const test_value_t &lhs, const test_value_t &rhs) {
  return lhs.ordering_value < rhs.ordering_value;
}

class NotBeforeTest : public testing::Test {
  not_before_queue_t<test_value_t> queue;
public:
};

TEST_F(NotBeforeTest, Basic) {
}
