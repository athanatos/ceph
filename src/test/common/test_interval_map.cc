// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include "include/buffer.h"
#include "common/interval_map.h"

using namespace std;

template<typename T>
class IntervalMapTest : public ::testing::Test {
public:
  using IMap = interval_map<
    typename T::key, typename T::value, typename T::splitter
    >;
  using TT = T;
};

template <typename _key>
struct bufferlist_test_type {
  using key = _key;
  using value = bufferlist;
  struct splitter {
    bufferlist operator()(
      key offset,
      key len,
      bufferlist &bu) {
      bufferlist bl;
      bl.substr_of(bu, offset, len);
      return bl;
    }
  };
  struct generate_random {
    bufferlist operator()(key len) {
      bufferlist bl;
      boost::random::mt19937 rng;
      boost::random::uniform_int_distribution<> chr(0,255);
      for (key i = 0; i < len; ++i) {
	bl.append((char)chr(rng));
      }
      return bl;
    }
  };
};

using IntervalMapTypes = ::testing::Types< bufferlist_test_type<uint64_t> >;

TYPED_TEST_CASE(IntervalMapTest, IntervalMapTypes);

#define USING \
  using key = typename TestFixture::TT::key; (void)key(0);	 \
  using imap = typename TestFixture::IMap; (void)imap();	 \
  typename TestFixture::TT::generate_random gen;                 \
  using val = typename TestFixture::TT::value; val v(gen(5));	 \
  typename TestFixture::TT::splitter split; (void)split(0, 0, v);

TYPED_TEST(IntervalMapTest, empty) {
  USING;
  imap m;
  ASSERT_TRUE(m.empty());
}

TYPED_TEST(IntervalMapTest, insert) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(10, 5, vals[2]);
  m.insert(5, 5, vals[1]);
  ASSERT_EQ(m.ext_count(), 3);

  unsigned i = 0;
  for (auto &&ext: m) {
    ASSERT_EQ(ext.get_len(), 5);
    ASSERT_EQ(ext.get_off(), 5 * i);
    ASSERT_EQ(ext.get_val(), vals[i]);
    ++i;
  }
  ASSERT_EQ(i, m.ext_count());
}

TYPED_TEST(IntervalMapTest, insert_begin_overlap) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);
  m.insert(1, 5, vals[0]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 1);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 6);
  ASSERT_EQ(iter.get_len(), 4);
  ASSERT_EQ(iter.get_val(), split(1, 4, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_end_overlap) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(8, 5, vals[2]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5);
  ASSERT_EQ(iter.get_len(), 3);
  ASSERT_EQ(iter.get_val(), split(0, 3, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 8);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_middle_overlap) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(7), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(10, 5, vals[2]);
  m.insert(4, 7, vals[1]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 4);
  ASSERT_EQ(iter.get_val(), split(0, 4, vals[0]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 4);
  ASSERT_EQ(iter.get_len(), 7);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 11);
  ASSERT_EQ(iter.get_len(), 4);
  ASSERT_EQ(iter.get_val(), split(1, 4, vals[2]));
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_single_exact_overlap) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, gen(5));
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);
  m.insert(0, 5, vals[0]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, insert_single_exact_overlap_end) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, gen(5));
  m.insert(10, 5, vals[2]);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 5);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, erase) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);

  m.erase(3, 5);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 3);
  ASSERT_EQ(iter.get_val(), split(0, 3, vals[0]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 8);
  ASSERT_EQ(iter.get_len(), 2);
  ASSERT_EQ(iter.get_val(), split(3, 2, vals[1]));
  ++iter;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, erase_exact) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5)};
  m.insert(0, 5, vals[0]);
  m.insert(5, 5, vals[1]);
  m.insert(10, 5, vals[2]);

  m.erase(5, 5);

  auto iter = m.begin();
  ASSERT_EQ(iter.get_off(), 0);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[0]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, m.end());
}

TYPED_TEST(IntervalMapTest, get_containing_range) {
  USING;
  imap m;
  vector<val> vals{gen(5), gen(5), gen(5), gen(5)};
  m.insert(0, 5, gen(5));
  m.insert(10, 5, vals[1]);
  m.insert(20, 5, vals[2]);
  m.insert(30, 5, vals[3]);

  auto rng = m.get_containing_range(5, 21);
  auto iter = rng.first;

  ASSERT_EQ(iter.get_off(), 10);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[1]);
  ++iter;

  ASSERT_EQ(iter.get_off(), 20);
  ASSERT_EQ(iter.get_len(), 5);
  ASSERT_EQ(iter.get_val(), vals[2]);
  ++iter;

  ASSERT_EQ(iter, rng.second);
}
