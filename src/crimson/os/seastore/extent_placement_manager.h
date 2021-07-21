// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/common/log.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

class Transaction;
struct empty_hint_t {};

/**
 * ool_record_t
 *
 * Encapsulates logic for building and encoding an ool record destined for
 * an ool segment.
 *
 * Uses a metadata header to enable scanning the ool segment for gc purposes.
 * Introducing a seperate physical->logical mapping would enable removing the
 * metadata block overhead.
 */
class ool_record_t {
public:
  ool_record_t(size_t block_size) : block_size(block_size) {}
  record_size_t get_encoded_record_length() {
    return crimson::os::seastore::get_encoded_record_length(record, block_size);
  }
  size_t get_wouldbe_encoded_record_length(LogicalCachedExtentRef& extent) {
    auto raw_mdlength = get_encoded_record_raw_mdlength(record, block_size);
    auto wouldbe_mdlength = p2roundup(
      raw_mdlength + ceph::encoded_sizeof_bounded<extent_info_t>(),
      block_size);
    return wouldbe_mdlength + extent_buf_len + extent->get_bptr().length();
  }
  ceph::bufferlist encode(paddr_t base, segment_nonce_t nonce) {
    assert(extents.size() == record.extents.size());
    auto rsize = get_encoded_record_length();
    segment_off_t extent_offset = base.offset + rsize.mdlength;
    for (auto& extent : extents) {
      extent->set_rewriting_paddr(
        {base.segment, extent_offset});
      extent_offset += extent->get_bptr().length();
    }
    return encode_record(rsize, std::move(record), block_size, nonce);
  }
  void add_extent(LogicalCachedExtentRef& extent) {
    extents.emplace_back(extent);
    ceph::bufferlist bl;
    bl.append(extent->get_bptr());
    record.extents.emplace_back(extent_t{
      extent->get_type(),
      extent->get_laddr(),
      std::move(bl)});
    extent_buf_len += extent->get_bptr().length();
  }
  void clear() {
    record.extents.clear();
    extents.clear();
    assert(!record.deltas.size());
    extent_buf_len = 0;
  }
  uint64_t get_num_extents() {
    return extents.size();
  }
private:
  std::vector<LogicalCachedExtentRef> extents;
  record_t record;
  size_t block_size;
  segment_off_t extent_buf_len = 0;
};

/**
 * ExtentAllocator
 *
 * Handles allocating ool extents from a specific family of targets.
 */
class ExtentAllocator {
public:
  virtual CachedExtentRef alloc_ool_extent(
    Transaction& t,
    extent_types_t type,
    segment_off_t length,
    ool_placement_hint_t hint = ool_placement_hint_t::NONE) = 0;

  virtual ~ExtentAllocator() {};
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

/**
 * ExtentOolWriter
 *
 * Interface through which final write to ool segment is performed.
 */
class ExtentOolWriter {
public:
  using write_iertr = trans_iertr<crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >>;

  virtual write_iertr::future<> write(std::list<LogicalCachedExtentRef>& extent) = 0;
  virtual ~ExtentOolWriter() {}
};

/**
 * SegmentedAllocator
 *
 * Handles out-of-line writes to a SegmentManager device (such as a ZNS device
 * or conventional flash device where sequential writes are heavily preferred).
 *
 * Creates <seastore_init_rewrite_segments_per_device> Writer instances
 * internally to round-robin writes.  Later work will partition allocations
 * based on hint (age, presumably) among the created Writers.

 * Each Writer makes use of SegmentProvider to obtain a new segment for writes
 * as needed.
 */
class SegmentedAllocator : public ExtentAllocator {
  class Writer : public ExtentOolWriter {
  public:
    Writer(SegmentProvider& sp, SegmentManager& sm)
      : segment_provider(sp), segment_manager(sm) {}
    Writer(Writer &&) = default;

    write_iertr::future<> write(std::list<LogicalCachedExtentRef>& extent) final;
  private:
    bool _needs_roll(segment_off_t length) const;

    using roll_segment_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    roll_segment_ertr::future<> roll_segment();

    using init_segment_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    init_segment_ertr::future<> init_segment(Segment& segment);

    using extents_to_write_t = std::vector<LogicalCachedExtentRef>;
    void add_extent_to_write(
      ool_record_t&,
      LogicalCachedExtentRef& extent);

    SegmentProvider& segment_provider;
    SegmentManager& segment_manager;
    SegmentRef current_segment;
    std::vector<SegmentRef> open_segments;
    segment_off_t allocated_to = 0;
  };
public:
  SegmentedAllocator(SegmentProvider& sp, SegmentManager& sm, Cache& cache);

  Writer &get_writer(ool_placement_hint_t hint) {
    return writers[std::rand() % writers.size()];
  }

  CachedExtentRef alloc_ool_extent(
    Transaction& t,
    extent_types_t type,
    segment_off_t length,
    ool_placement_hint_t hint) final {
    auto& writer = get_writer(hint);

    auto nextent = cache.alloc_new_extent_by_type(
      t, type, length, paddr_t{ZERO_SEG_ID, fake_paddr_off});
    fake_paddr_off += length;

    nextent->extent_writer = &writer;
    return nextent;
  }

private:
  segment_off_t fake_paddr_off = 0;

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::vector<Writer> writers;
  Cache& cache;
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager(
    Cache& cache,
    ExtentAllocatorRef &&allocator
  )
    : cache(cache), default_allocator(std::move(allocator)) {}

  CachedExtentRef alloc_new_extent_by_type(
    Transaction& t,
    extent_types_t type,
    segment_off_t length,
    ool_placement_hint_t hint = ool_placement_hint_t::NONE) {
    return default_allocator->alloc_ool_extent(t, type, length, hint);
  }

private:
  Cache& cache;

  /**
   * default_allocator
   *
   * Currently, we only allow a single allocator.  Later, once we other classes
   * of allocator we'll want a way to register multiple allocators with
   * associated hints.  We'll also need a way to deal with different allocators
   * being full, etc.
   */
  ExtentAllocatorRef default_allocator;
};
using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
