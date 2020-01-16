// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

namespace crimson::os::seastore {

using checksum_t = uint32_t;

using journal_seq_t = uint64_t;
constexpr journal_seq_t NO_DELTAS = std::numeric_limits<journal_seq_t>::max();

using segment_id_t = uint32_t;
using segment_off_t = uint32_t;
struct paddr_t {
  segment_id_t segment;
  segment_off_t offset;
};

using laddr_t = uint64_t;

constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

constexpr paddr_t P_ADDR_NULL = {
  std::numeric_limits<segment_id_t>::max(),
  std::numeric_limits<segment_off_t>::max()
};

}
