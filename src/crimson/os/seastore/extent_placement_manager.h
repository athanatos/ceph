// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/common/log.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

class Transaction;
struct empty_hint_t {};

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

class SegmentedOolWriter : public ExtentOolWriter,
                          public boost::intrusive_ref_counter<
  SegmentedOolWriter, boost::thread_unsafe_counter>{
public:
  using roll_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_ertr = roll_segment_ertr;
  using init_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  SegmentedOolWriter(SegmentProvider& sp, SegmentManager& sm)
    : segment_provider(sp), segment_manager(sm) {}
  write_iertr::future<> write(std::list<LogicalCachedExtentRef>& extent) final;

private:
  bool _needs_roll(segment_off_t length) const;
  roll_segment_ertr::future<> roll_segment();
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

using SegmentedOolWriterRef = std::unique_ptr<SegmentedOolWriter>;

class scan_valid_records_cursor;

template <typename HintT = empty_hint_t>
class ExtentAllocator {
public:
  using scan_device_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual CachedExtentRef alloc_ool_extent(
    Transaction& t,
    extent_types_t type,
    const HintT hint,
    segment_off_t length) = 0;

  virtual ~ExtentAllocator() {};
};

template <typename HintT = empty_hint_t>
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator<HintT>>;

template <typename IndexT, typename HintT = empty_hint_t>
class SegmentedAllocator : public ExtentAllocator<HintT> {
public:
  using calc_target_func_t = typename std::function<IndexT (HintT)>;
  using alloc_extent_ertr = SegmentedOolWriter::alloc_extent_ertr;
  SegmentedAllocator(
    SegmentProvider& sp,
    SegmentManager& sm,
    Cache& cache,
    calc_target_func_t&& calc_target_func)
    : segment_provider(sp),
      segment_manager(sm),
      cache(cache),
      calc_target_func(std::move(calc_target_func))
  {}

  CachedExtentRef alloc_ool_extent(
    Transaction& t,
    extent_types_t type,
    const HintT hint,
    segment_off_t length) final {
    auto index = calc_target_func(hint);
    auto iter = writers.find(index);
    if (iter == writers.end()) {
      iter = writers.emplace(
        index,
        std::make_unique<SegmentedOolWriter>(
          segment_provider,
          segment_manager)).first;
    }
    auto& writer = iter->second;

    auto nextent = cache.alloc_new_extent_by_type(
      t, type, length, paddr_t{ZERO_SEG_ID, fake_paddr_off});
    fake_paddr_off += length;
    nextent->extent_writer = writer.get();
    return nextent;

  }

  using read_segment_header_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::enodata,
    crimson::ct_error::input_output_error
    >;
  using read_segment_header_ret = read_segment_header_ertr::future<
    segment_header_t>;
  read_segment_header_ret read_segment_header(segment_id_t segment);

private:
  segment_off_t fake_paddr_off = 0;

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::map<IndexT, SegmentedOolWriterRef> writers;
  Cache& cache;
  calc_target_func_t calc_target_func;
};

template <typename IndexT, typename HintT = empty_hint_t>
class ExtentPlacementManager {
public:
  using calc_target_func_t =
    typename std::function<
      IndexT (HintT, ExtentPlacementManager<IndexT, HintT>&)>;
  using alloc_extent_ertr = SegmentedOolWriter::alloc_extent_ertr;

  ExtentPlacementManager(Cache& cache, calc_target_func_t&& calc_target_func)
    : cache(cache), calc_target_func(std::move(calc_target_func)) {}

  CachedExtentRef alloc_new_extent_by_type(
    Transaction& t,
    extent_types_t type,
    const HintT& hint,
    segment_off_t length) {
    auto h = calc_target_func(hint, *this);
    auto iter = extent_allocators.find(h);
    auto& allocator = iter->second;

    return allocator->alloc_ool_extent(t, type, hint, length);
  }

  void add_allocator(IndexT hl, ExtentAllocatorRef<HintT>&& allocator) {
    auto [it, inserted] = extent_allocators.emplace(hl, std::move(allocator));
    assert(inserted);
  }

  uint64_t get_num_allocators() {
    return extent_allocators.size();
  }

private:
  std::map<IndexT, ExtentAllocatorRef<HintT>> extent_allocators;
  Cache& cache;
  calc_target_func_t calc_target_func;
};

template <typename IndexT, typename HintT = empty_hint_t>
using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager<IndexT, HintT>>;

}
