// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <iostream>

#include "include/byteorder.h"
#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"
#include "include/uuid.h"

namespace crimson::os::seastore {

using depth_t = int32_t;
using depth_le_t = ceph_les32;

using checksum_t = uint32_t;

// Immutable metadata for seastore to set at mkfs time
struct seastore_meta_t {
  uuid_d seastore_id;

  DENC(seastore_meta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seastore_id, p);
    DENC_FINISH(p);
  }
};

// Identifies segment location on disk, see SegmentManager,
using segment_id_t = uint32_t;
constexpr segment_id_t NULL_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 1;
/* Used to denote relative paddr_t */
constexpr segment_id_t RECORD_REL_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 2;
constexpr segment_id_t BLOCK_REL_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 3;

// for tests which generate fake paddrs
constexpr segment_id_t FAKE_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 4;

std::ostream &segment_to_stream(std::ostream &, const segment_id_t &t);

// Offset within a segment on disk, see SegmentManager
// may be negative for relative offsets
using segment_off_t = int32_t;
constexpr segment_off_t NULL_SEG_OFF =
  std::numeric_limits<segment_id_t>::max();

std::ostream &offset_to_stream(std::ostream &, const segment_off_t &t);

/* Monotonically increasing segment seq, uniquely identifies
 * the incarnation of a segment */
using segment_seq_t = uint32_t;
static constexpr segment_seq_t NULL_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();

// Offset of delta within a record
using record_delta_idx_t = uint32_t;
constexpr record_delta_idx_t NULL_DELTA_IDX =
  std::numeric_limits<record_delta_idx_t>::max();

/**
 * paddr_t
 *
 * <segment, offset> offset on disk, see SegmentManager
 *
 * May be absolute, record_relative, or block_relative.
 *
 * Blocks get read independently of the surrounding record,
 * so paddrs embedded directly within a block need to refer
 * to other blocks within the same record by a block_relative
 * addr relative to the block's own offset.  By contrast,
 * deltas to existing blocks need to use record_relative
 * addrs relative to the first block of the record.
 *
 * Fresh extents during a transaction are refered to by
 * record_relative paddrs.
 */
struct paddr_t {
  segment_id_t segment = NULL_SEG_ID;
  segment_off_t offset = NULL_SEG_OFF;

  bool is_relative() const {
    return segment == RECORD_REL_SEG_ID ||
      segment == BLOCK_REL_SEG_ID;
  }

  bool is_record_relative() const {
    return segment == RECORD_REL_SEG_ID;
  }

  bool is_block_relative() const {
    return segment == BLOCK_REL_SEG_ID;
  }

  paddr_t add_offset(segment_off_t o) const {
    return paddr_t{segment, offset + o};
  }

  paddr_t add_relative(paddr_t o) const {
    assert(o.is_relative());
    return paddr_t{segment, offset + o.offset};
  }

  paddr_t add_block_relative(paddr_t o) const {
    // special version mainly for documentation purposes
    assert(o.is_block_relative());
    return add_relative(o);
  }

  paddr_t add_record_relative(paddr_t o) const {
    // special version mainly for documentation purposes
    assert(o.is_record_relative());
    return add_relative(o);
  }

  /**
   * paddr_t::operator-
   *
   * Only defined for record_relative paddr_ts.  Yields a
   * block_relative address.
   */
  paddr_t operator-(paddr_t rhs) const {
    assert(rhs.is_relative() && is_relative());
    assert(rhs.segment == segment);
    return paddr_t{
      BLOCK_REL_SEG_ID,
      offset - rhs.offset
    };
  }

  /**
   * maybe_relative_to
   *
   * Helper for the case where an in-memory paddr_t may be
   * either block_relative or absolute (not record_relative).
   *
   * base must be either absolute or record_relative.
   */
  paddr_t maybe_relative_to(paddr_t base) const {
    assert(!base.is_block_relative());
    if (is_block_relative())
      return base.add_block_relative(*this);
    else
      return *this;
  }

  DENC(paddr_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment, p);
    denc(v.offset, p);
    DENC_FINISH(p);
  }
};
WRITE_CMP_OPERATORS_2(paddr_t, segment, offset)
WRITE_EQ_OPERATORS_2(paddr_t, segment, offset)
constexpr paddr_t P_ADDR_NULL = paddr_t{};
constexpr paddr_t P_ADDR_MIN = paddr_t{0, 0};
constexpr paddr_t make_record_relative_paddr(segment_off_t off) {
  return paddr_t{RECORD_REL_SEG_ID, off};
}
constexpr paddr_t make_block_relative_paddr(segment_off_t off) {
  return paddr_t{BLOCK_REL_SEG_ID, off};
}
constexpr paddr_t make_fake_paddr(segment_off_t off) {
  return paddr_t{FAKE_SEG_ID, off};
}

struct paddr_le_t {
  ceph_le32 segment = init_le32(NULL_SEG_ID);
  ceph_les32 offset = init_les32(NULL_SEG_OFF);

  paddr_le_t() = default;
  paddr_le_t(ceph_le32 segment, ceph_les32 offset)
    : segment(segment), offset(offset) {}
  paddr_le_t(segment_id_t segment, segment_off_t offset)
    : segment(init_le32(segment)), offset(init_les32(offset)) {}
  paddr_le_t(const paddr_t &addr) : paddr_le_t(addr.segment, addr.offset) {}

  operator paddr_t() const {
    return paddr_t{segment, offset};
  }
};

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs);

using objaddr_t = uint32_t;
constexpr objaddr_t OBJ_ADDR_MIN = std::numeric_limits<objaddr_t>::min();

/* Monotonically increasing identifier for the location of a
 * journal_record.
 */
struct journal_seq_t {
  segment_seq_t segment_seq = 0;
  paddr_t offset;

  DENC(journal_seq_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment_seq, p);
    denc(v.offset, p);
    DENC_FINISH(p);
  }
};
WRITE_CMP_OPERATORS_2(journal_seq_t, segment_seq, offset)
WRITE_EQ_OPERATORS_2(journal_seq_t, segment_seq, offset)

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq);

static constexpr journal_seq_t NO_DELTAS = journal_seq_t{
  NULL_SEG_SEQ,
  P_ADDR_NULL
};

// logical addr, see LBAManager, TransactionManager
using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_MIN = std::numeric_limits<laddr_t>::min();
constexpr laddr_t L_ADDR_MAX = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

struct laddr_le_t {
  ceph_le64 laddr = init_le64(L_ADDR_NULL);

  laddr_le_t() = default;
  laddr_le_t(const laddr_le_t &) = default;
  explicit laddr_le_t(const laddr_t &addr)
    : laddr(init_le64(addr)) {}

  operator laddr_t() const {
    return laddr_t(laddr);
  }
  laddr_le_t& operator=(laddr_t addr) {
    ceph_le64 val;
    val = addr;
    laddr = val;
    return *this;
  }
};

// logical offset, see LBAManager, TransactionManager
using extent_len_t = uint32_t;
constexpr extent_len_t EXTENT_LEN_MAX =
  std::numeric_limits<extent_len_t>::max();

using extent_len_le_t = ceph_le32;
inline extent_len_le_t init_extent_len_le_t(extent_len_t len) {
  return init_le32(len);
}

struct laddr_list_t : std::list<std::pair<laddr_t, extent_len_t>> {
  template <typename... T>
  laddr_list_t(T&&... args)
    : std::list<std::pair<laddr_t, extent_len_t>>(std::forward<T>(args)...) {}
};
struct paddr_list_t : std::list<std::pair<paddr_t, extent_len_t>> {
  template <typename... T>
  paddr_list_t(T&&... args)
    : std::list<std::pair<paddr_t, extent_len_t>>(std::forward<T>(args)...) {}
};

std::ostream &operator<<(std::ostream &out, const laddr_list_t &rhs);
std::ostream &operator<<(std::ostream &out, const paddr_list_t &rhs);

/* identifies type of extent, used for interpretting deltas, managing
 * writeback.
 *
 * Note that any new extent type needs to be added to
 * Cache::get_extent_by_type in cache.cc
 */
enum class extent_types_t : uint8_t {
  ROOT = 0,
  LADDR_INTERNAL = 1,
  LADDR_LEAF = 2,
  EXTMAP_INNER = 4,
  EXTMAP_LEAF = 5,
  OMAP_INNER = 6,
  OMAP_LEAF = 7,
  ONODE_BLOCK_STAGED = 8,
  COLL_BLOCK = 9,

  // Test Block Types
  TEST_BLOCK = 0xF0,
  TEST_BLOCK_PHYSICAL = 0xF1,

  // None
  NONE = 0xFF
};

inline bool is_logical_type(extent_types_t type) {
  switch (type) {
  case extent_types_t::ROOT:
  case extent_types_t::LADDR_INTERNAL:
  case extent_types_t::LADDR_LEAF:
    return false;
  default:
    return true;
  }
}

std::ostream &operator<<(std::ostream &out, extent_types_t t);

/* description of a new physical extent */
struct extent_t {
  extent_types_t type;  ///< type of extent
  laddr_t addr;         ///< laddr of extent (L_ADDR_NULL for non-logical)
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using extent_version_t = uint32_t;
constexpr extent_version_t EXTENT_VERSION_NULL = 0;

/* description of a mutation to a physical extent */
struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  paddr_t paddr;                               ///< physical address
  laddr_t laddr = L_ADDR_NULL;                 ///< logical address
  uint32_t prev_crc = 0;
  uint32_t final_crc = 0;
  segment_off_t length = NULL_SEG_OFF;         ///< extent length
  extent_version_t pversion;                   ///< prior version
  ceph::bufferlist bl;                         ///< payload

  DENC(delta_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.paddr, p);
    denc(v.laddr, p);
    denc(v.prev_crc, p);
    denc(v.final_crc, p);
    denc(v.length, p);
    denc(v.pversion, p);
    denc(v.bl, p);
    DENC_FINISH(p);
  }

  bool operator==(const delta_info_t &rhs) const {
    return (
      type == rhs.type &&
      paddr == rhs.paddr &&
      laddr == rhs.laddr &&
      prev_crc == rhs.prev_crc &&
      final_crc == rhs.final_crc &&
      length == rhs.length &&
      pversion == rhs.pversion &&
      bl == rhs.bl
    );
  }

  friend std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs);
};

std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs);

struct record_t {
  std::vector<extent_t> extents;
  std::vector<delta_info_t> deltas;
};

/**
 * coll_root_t
 *
 * Information for locating CollectionManager information, addr should be
 * embedded into the TransactionManager root.
 */
class coll_root_t {
  laddr_t coll_root_laddr;
  segment_off_t size = 0;
  
  enum state_t : uint8_t {
    CLEAN = 0,   /// No pending mutations
    MUTATED = 1, /// coll_root_laddr state must be written back to persistence
    NONE = 0xFF  /// Not yet mounted, should not be exposed to user
  } state = NONE;
  
public:
  coll_root_t() : state(state_t::NONE) {}
  
  coll_root_t(laddr_t laddr, segment_off_t size)
    : coll_root_laddr(laddr), size(size), state(state_t::CLEAN) {}
  
  bool must_update() const {
    return state == MUTATED;
  }
  
  void update(laddr_t addr, segment_off_t s) {
    state = state_t::MUTATED;
    coll_root_laddr = addr;
    size = s;
  }
  
  laddr_t get_location() const {
    return coll_root_laddr;
  }
  auto get_size() const {
    return size;
  }
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::seastore_meta_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::journal_seq_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
