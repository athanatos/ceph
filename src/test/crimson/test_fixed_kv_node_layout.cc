// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include "gtest/gtest.h"

#include "crimson/common/fixed_kv_node_layout.h"

using namespace crimson;
using namespace crimson::common;

struct test_val_t {
  uint32_t t1 = 0;
  int32_t t2 = 0;

  bool operator==(const test_val_t &rhs) const {
    return rhs.t1 == t1 && rhs.t2 == t2;
  }
};

struct test_val_le_t {
  ceph_le32 t1 = init_le32(0);
  ceph_les32 t2 = init_les32(0);

  test_val_le_t() = default;
  test_val_le_t(const test_val_le_t &) = default;
  test_val_le_t(const test_val_t &nv)
    : t1(init_le32(nv.t1)), t2(init_les32(nv.t2)) {}

  operator test_val_t() {
    return test_val_t{t1, t2};
  }
};

constexpr size_t CAPACITY = 341;

struct TestNode : FixedKVNodeLayout<
  CAPACITY,
  uint32_t, ceph_le32,
  test_val_t, test_val_le_t> {
  char buf[4096];
  TestNode() : FixedKVNodeLayout(buf) {
    memset(buf, 0, sizeof(buf));
  }
};

TEST(FixedKVNodeTest, basic) {
  auto node = TestNode();
  ASSERT_EQ(node.get_size(), 0);
  node.set_size(1);
  ASSERT_EQ(node.get_size(), 1);

  auto iter = node.begin();
  iter.set_key(1);
  ASSERT_EQ(iter.get_key(), 1);

  auto val = test_val_t{ 1, 1 };
  iter.set_val(val);
  ASSERT_EQ(val, iter.get_val());

  ASSERT_EQ(std::numeric_limits<uint32_t>::max(), iter.get_next_key_or_max());
}

TEST(FixedKVNodeTest, at_capacity) {
  auto node = TestNode();
  ASSERT_EQ(CAPACITY, node.get_capacity());

  ASSERT_EQ(node.get_size(), 0);
  node.set_size(node.get_capacity());
  ASSERT_EQ(node.get_size(), CAPACITY);

  unsigned short num = 0;
  for (auto &i : node) {
    i.set_key(num);
    i.set_val({ num, num});
    ++num;
  }

  num = 0;
  for (auto &i : node) {
    ASSERT_EQ(i.get_key(), num);
    ASSERT_EQ(i.get_val(), (test_val_t{num, num}));
    if (num < (CAPACITY - 1)) {
      ASSERT_EQ(i.get_next_key_or_max(), num + 1);
    } else {
      ASSERT_EQ(std::numeric_limits<uint32_t>::max(), i.get_next_key_or_max());
    }
    ++num;
  }
}
