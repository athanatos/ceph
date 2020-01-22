// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

#include "include/denc.h"
#include "include/buffer.h"

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

enum class extent_types_t : uint8_t {
  ROOT = 0,
  LADDR_TREE = 1,
  LBA_BLOCK = 2,
  NONE = 0xFF
};

struct delta_info_t {
  extent_types_t type;  ///< delta type
  laddr_t laddr;        ///< logical address, null iff delta != LBA_BLOCK
  paddr_t paddr;        ///< physical address
  segment_off_t length; ///< extent length
  ceph::bufferlist bl;  ///< payload

  DENC(delta_info_t, v, p) {
    denc(v.type, p);
    denc(v.laddr, p);
    denc(v.paddr, p);
    denc(v.length, p);
    denc(v.bl, p);
  }
};

struct extent_info_t {
  extent_types_t type;  ///< delta type
  laddr_t laddr;        ///< logical address, null iff delta != LBA_BLOCK
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using journal_seq_t = uint64_t;
static constexpr journal_seq_t NO_DELTAS =
  std::numeric_limits<journal_seq_t>::max();

struct record_t {
  std::vector<extent_info_t> extents;
  std::vector<delta_info_t> deltas;
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
