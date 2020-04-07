// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::lba_manager;
using namespace crimson::os::seastore::lba_manager::btree;

struct btree_lba_manager_test_t : public seastar_test_case_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Cache cache;
  BtreeLBAManager lba_manager;

  btree_lba_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      cache(*segment_manager),
      lba_manager(*segment_manager, cache) {}
};

TEST_F(btree_lba_manager_test_t, basic)
{
  // TODO
}

