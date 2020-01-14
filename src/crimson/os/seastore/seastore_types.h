// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

namespace crimson::os::seastore {

using segment_id_t = uint32_t;
using segment_off_t = uint32_t;
struct paddr_t {
  segment_id_t segment;
  segment_off_t offset;
};

using laddr_t = uint64_t;

constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr paddr_t P_ADDR_NULL = {
  std::numeric_limits<segment_id_t>::max(),
  std::numeric_limits<segment_off_t>::max()
};

}
