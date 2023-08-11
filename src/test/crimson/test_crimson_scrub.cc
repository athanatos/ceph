// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/sleep.hh>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/interruptible_future.h"
#include "crimson/common/log.h"
#include "crimson/osd/scrub/scrub_machine.h"
#include "crimson/osd/scrub/scrub_validator.h"

using namespace crimson;

TEST(crimson_scrub, basic) {
  crimson::osd::scrub::chunk_validation_policy_t policy;
  crimson::osd::scrub::scrub_map_set_t in;
  auto ret = crimson::osd::scrub::validate_chunk(policy, in);
  std::ignore = ret;
}
