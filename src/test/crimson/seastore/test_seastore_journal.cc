// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

struct journal_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Journal journal;

  journal_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager) {}
 
  seastar::future<> set_up_fut() final {
    return seastar::now();
  }

  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }
};

TEST_F(journal_test_t, basic)
{
 run([] { return seastar::now(); });
}

