// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>

#include "common/scrub_types.h"
#include "osd/osd_types.h"

namespace crimson::osd::scrub {

struct chunk_validation_policy_t {
  bool is_ec{false};
};

using scrub_map_set_t = std::map<pg_shard_t, ScrubMap>;

struct chunk_info_t {
  scrub_map_set_t maps;
};

struct chunk_result_t {
  unsigned shallow_errors{0};
  unsigned deep_errors{0};

  // omap specific stats
  uint64_t large_omap_objects{0};
  uint64_t omap_bytes{0};
  uint64_t omap_keys{0};

  // detected errors
  std::vector<inconsistent_snapset_wrapper> snapset_errors;
  std::vector<inconsistent_obj_wrapper> object_errors;
};

chunk_result_t validate_chunk(
  const chunk_info_t &in);

}
