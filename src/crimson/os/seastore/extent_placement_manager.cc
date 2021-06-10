// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/object_data_handler.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

void SegmentedOolWriter::add_extent_to_write(
  ool_record_t& record,
  LogicalCachedExtentRef& extent) {
  extent->prepare_write();
  // all io_wait_promise set here will be completed when the corresponding
  // transaction "complete_commit"s, we can't do complete_io() here, otherwise,
  // there might be consistency problem.
  extent->set_io_wait();
  record.add_extent(extent);
}

SegmentedOolWriter::write_iertr::future<>
SegmentedOolWriter::write(std::list<LogicalCachedExtentRef>& extents) {
  logger().debug("{}", __func__);
  auto fut = roll_segment_ertr::now();
  if (!current_segment) {
    fut = roll_segment();
  }
  return fut.safe_then([this, &extents] {
    return seastar::do_with(ool_record_t(segment_manager.get_block_size()),
      [this, &extents](auto& record) {
      return crimson::do_for_each(
        extents,
        [this, &record](auto& extent)
        -> Segment::write_ertr::future<> {
        // if we meet the end of the current segment
        // before iterating through all extents, write them now.
        if (_needs_roll(record.get_wouldbe_encoded_record_length(extent))) {
          logger().debug(
            "SegmentedOolWriter::write: end of segment, writing {} extents to segment {} at {}",
            record.get_num_extents(),
            current_segment->get_segment_id(),
            allocated_to);
          bufferlist bl = record.encode(
            {current_segment->get_segment_id(), allocated_to}, 0);
          return current_segment->write(allocated_to, bl).safe_then(
            [this, &extent, &record] {
            record.clear();
            return roll_segment().safe_then([this, &extent, &record] {
              add_extent_to_write(record, extent);
              extent->extent_writer = nullptr;
              return seastar::now();
            });
          });
        }
        add_extent_to_write(record, extent);
        extent->extent_writer = nullptr;
        return seastar::now();
      }).safe_then([this, &record]()
        -> write_iertr::future<> {

        if (!record.get_num_extents()) {
          return seastar::now();
        }

        logger().debug(
          "SegmentedOolWriter::write: writing {} extents to segment {} at {}",
          record.get_num_extents(),
          current_segment->get_segment_id(),
          allocated_to);
        bufferlist bl = record.encode(
          {current_segment->get_segment_id(), allocated_to}, 0);
        return current_segment->write(allocated_to, bl).safe_then(
          [this, len=bl.length(), &record] {
          logger().debug(
            "SegmentedOolWriter::write: written {} extents,"
            " {} bytes to segment {} at {}",
            record.get_num_extents(),
            len,
            current_segment->get_segment_id(),
            allocated_to);

          allocated_to += len;
          return seastar::now();
        });
      });
    });
  });
}

bool SegmentedOolWriter::_needs_roll(segment_off_t length) const {
  return allocated_to + length > current_segment->get_write_capacity();
}

SegmentedOolWriter::init_segment_ertr::future<>
SegmentedOolWriter::init_segment(Segment& segment) {
  logger().debug("SegmentedOolWriter::init_segment: initting {}", segment.get_segment_id());
  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto header =segment_header_t{
    0, segment.get_segment_id(), 0, 0, true};
  ceph::bufferlist bl;
  encode(header, bl);
  bl.cbegin().copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);
  allocated_to = segment_manager.get_block_size();
  return segment.write(0, bl).handle_error(
    crimson::ct_error::input_output_error::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error when initing segment"}
  );
}

SegmentedOolWriter::roll_segment_ertr::future<>
SegmentedOolWriter::roll_segment() {
  return segment_provider.get_segment().safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto segref) {
    return init_segment(*segref).safe_then([segref=std::move(segref), this] {
      current_segment = segref;
      open_segments.emplace_back(segref);
    });
  }).handle_error(
    roll_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
  );
}

template class SegmentedAllocator<uint64_t>;
template class ExtentPlacementManager<uint64_t, empty_hint_t>;

}
