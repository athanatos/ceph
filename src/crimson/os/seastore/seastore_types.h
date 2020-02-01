// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <type_traits>

#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"

namespace crimson::os::seastore {

using segment_seq_t = uint32_t;
static constexpr segment_seq_t NO_SEGMENT =
  std::numeric_limits<segment_seq_t>::max();

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

using loff_t = uint64_t;
constexpr loff_t L_OFF_NULL = std::numeric_limits<laddr_t>::max();

enum class extent_types_t : uint8_t {
  ROOT = 0x0,
  LADDR_TREE = 0x1,
  LBA_BLOCK = 0x80,
  NONE = 0xFF
};

bool extent_type_is_physical(extent_types_t in) {
  return static_cast<uint8_t>(in) < 
    static_cast<uint8_t>(extent_types_t::LBA_BLOCK);
}

struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  union delta_addr_t {
    laddr_t laddr; ///< logical address, present iff delta == LBA_BLOCK
    paddr_t paddr; ///< physical address, present otherwise

    delta_addr_t(laddr_t addr) : laddr(addr) {}
    delta_addr_t(paddr_t addr) : paddr(addr) {}
  } addr;
  segment_off_t length = 0; ///< extent length
  ceph::bufferlist bl;  ///< payload

  DENC(delta_info_t, v, p) {
    denc(v.type, p);
    if (extent_type_is_physical(v.type)) {
      denc(v.addr.paddr, p);
    } else {
      denc(v.addr.laddr, p);
    }
    denc(v.length, p);
    denc(v.bl, p);
  }

  bool is_physical() const {
    return extent_type_is_physical(type);
  }

  delta_info_t() : addr{L_ADDR_NULL} {}

  delta_info_t(extent_types_t type, laddr_t inaddr,
	       segment_off_t length, ceph::bufferlist &&bl)
    : type(type), addr{inaddr}, length(length), bl(std::move(bl)) {
    ceph_assert(!is_physical());
  }

  delta_info_t(extent_types_t type, paddr_t inaddr,
	       segment_off_t length, ceph::bufferlist &&bl)
    : type(type), addr{inaddr}, length(length), bl(std::move(bl)) {
    ceph_assert(is_physical());
  }
};

struct extent_info_t {
  extent_types_t type;  ///< delta type
  laddr_t laddr;        ///< logical address, null iff delta != LBA_BLOCK
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

struct extent_version_t {
  segment_seq_t seq = NO_SEGMENT;
  segment_off_t off = NULL_SEG_OFF;

  DENC(extent_version_t, v, p) {
    denc(v.seq, p);
    denc(v.off, p);
  }
};
constexpr extent_version_t EXTENT_VERSION_NULL = extent_version_t{};
WRITE_CMP_OPERATORS_2(extent_version_t, seq, off)

using journal_seq_t = uint64_t;
static constexpr journal_seq_t NO_DELTAS =
  std::numeric_limits<journal_seq_t>::max();

struct record_t {
  std::vector<extent_info_t> extents;
  std::vector<delta_info_t> deltas;
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC(crimson::os::seastore::delta_info_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_version_t)
