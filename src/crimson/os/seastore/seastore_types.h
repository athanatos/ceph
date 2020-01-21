// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

#include "include/denc.h"

namespace crimson::os::seastore {

using checksum_t = uint32_t;

using segment_id_t = uint32_t;
constexpr segment_id_t NULL_SEG_ID = std::numeric_limits<segment_id_t>::max();

using segment_off_t = uint32_t;
constexpr segment_off_t NULL_SEG_OFF = std::numeric_limits<segment_id_t>::max();

struct paddr_t {
  segment_id_t segment = NULL_SEG_ID;
  segment_off_t offset = NULL_SEG_OFF;

  DENC(paddr_t, v, p) {
    denc(v.segment, p);
    denc(v.offset, p);
  }
};

constexpr paddr_t P_ADDR_NULL = paddr_t{};

using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

}

WRITE_CLASS_DENC(crimson::os::seastore::paddr_t)
