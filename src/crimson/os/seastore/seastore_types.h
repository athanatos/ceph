// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"

namespace crimson::os::seastore {

using checksum_t = uint32_t;

// Identifies segment location on disk, see SegmentManager,
// may be negative for relative offsets
using segment_id_t = int32_t; 
constexpr segment_id_t NULL_SEG_ID =
  std::numeric_limits<segment_id_t>::min();
/* Used to denote relative paddr_t */
constexpr segment_id_t REL_SEG_ID =
  std::numeric_limits<segment_id_t>::min() + 1;

// Offset within a segment on disk, see SegmentManager
using segment_off_t = uint32_t;
constexpr segment_off_t NULL_SEG_OFF =
  std::numeric_limits<segment_id_t>::max();

/* Monotonically increasing segment seq, uniquely identifies
 * the incarnation of a segment */
using segment_seq_t = uint32_t;
static constexpr segment_seq_t NULL_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();

// Offset of delta within a record
using record_delta_idx_t = uint32_t;
constexpr record_delta_idx_t NULL_DELTA_IDX =
  std::numeric_limits<record_delta_idx_t>::max();

// <segment, offset> offset on disk, see SegmentManager
struct paddr_t {
  segment_id_t segment = NULL_SEG_ID;
  segment_off_t offset = NULL_SEG_OFF;

  DENC(paddr_t, v, p) {
    denc(v.segment, p);
    denc(v.offset, p);
  }
};
WRITE_CMP_OPERATORS_2(paddr_t, segment, offset)
WRITE_EQ_OPERATORS_2(paddr_t, segment, offset)
constexpr paddr_t P_ADDR_NULL = paddr_t{};
paddr_t make_relative_paddr(segment_off_t off) {
  return paddr_t{REL_SEG_ID, off};
}

// logical addr, see LBAManager, TransactionManager
using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

// logical offset, see LBAManager, TransactionManager
using loff_t = uint64_t;
constexpr loff_t L_OFF_NULL = std::numeric_limits<laddr_t>::max();

using laddr_list_t = std::list<std::pair<laddr_t, loff_t>>;
using paddr_list_t = std::list<std::pair<paddr_t, segment_off_t>>;

/* identifies type of extent, used for interpretting deltas, managing
 * writeback */
enum class extent_types_t : uint8_t {
  ROOT = 0,
  LADDR_TREE = 1,
  LBA_BLOCK = 2,
  NONE = 0xFF
};

/* description of a new physical extent */
struct extent_t {
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using extent_version_t = uint32_t;
constexpr extent_version_t EXTENT_VERSION_NULL = 0;

/* description of a mutation to a physical extent */
struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  paddr_t paddr;                               ///< physical address
  /* logical address -- needed for repopulating cache */
  laddr_t laddr = L_ADDR_NULL;
  segment_off_t length = NULL_SEG_OFF;         ///< extent length
  extent_version_t pversion;                   ///< prior version
  ceph::bufferlist bl;                         ///< payload

  DENC(delta_info_t, v, p) {
    denc(v.type, p);
    denc(v.paddr, p);
    denc(v.laddr, p);
    denc(v.length, p);
    denc(v.pversion, p);
    denc(v.bl, p);
  }
};

struct record_t {
  std::vector<extent_t> extents;
  std::vector<delta_info_t> deltas;
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
