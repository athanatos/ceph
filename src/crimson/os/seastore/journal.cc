// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/os/seastore/journal.h"

#include "include/intarith.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

Journal::Journal(SegmentManager &segment_manager)
  : block_size(segment_manager.get_block_size()),
    max_record_length(
      segment_manager.get_segment_size() -
      p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
	      size_t(block_size))),
    segment_manager(segment_manager) {}


Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment)
{
  logger().debug("initialize_segment {}", segment.get_segment_id());
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;
  auto header = segment_header_t{
    current_journal_segment_seq++,
    segment.get_segment_id(),
    current_replay_point};
  ::encode(header, bl);

  written_to = segment_manager.get_block_size();
  return segment.write(0, bl).handle_error(
    init_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); }));
}

ceph::bufferlist Journal::encode_record(
  segment_off_t mdlength,
  segment_off_t dlength,
  record_t &&record)
{
  bufferlist metadatabl;
  record_header_t header{
    mdlength,
    dlength,
    current_journal_seq,
    0 /* checksum, TODO */,
    record.deltas.size(),
    record.extents.size()
  };
  ::encode(header, metadatabl);
  for (const auto &i: record.deltas) {
    ::encode(i, metadatabl);
  }
  bufferlist databl;
  for (auto &i: record.extents) {
    databl.claim_append(i.bl);
  }
  if (metadatabl.length() % block_size != 0) {
    metadatabl.append(
      ceph::bufferptr(
	block_size - (metadatabl.length() % block_size)));
  }
  ceph_assert(metadatabl.length() == mdlength);
  ceph_assert(databl.length() == dlength);
  metadatabl.claim_append(databl);
  ceph_assert(metadatabl.length() == (mdlength + dlength));
  return metadatabl;
}

Journal::write_record_ertr::future<> Journal::write_record(
  segment_off_t mdlength,
  segment_off_t dlength,
  record_t &&record)
{
  ceph::bufferlist to_write = encode_record(
    mdlength, dlength, std::move(record));
  written_to += p2roundup(to_write.length(), block_size);
  return current_journal_segment->write(written_to, to_write).handle_error(
    write_record_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); }));
    
}

std::pair<segment_off_t, segment_off_t> Journal::get_encoded_record_length(
  const record_t &record) const {
  auto metadata = ceph::encoded_sizeof_bounded<record_header_t>();
  auto data = 0;
  for (const auto &i: record.deltas) {
    metadata += ceph::encoded_sizeof(i);
  }
  for (const auto &i: record.extents) {
    data += i.bl.length();
  }
  metadata = p2roundup(metadata, (size_t)block_size);
  return std::make_pair(metadata, data);
}

bool Journal::needs_roll(segment_off_t length) const
{
  return length + written_to >
    current_journal_segment->get_write_capacity();
}

paddr_t Journal::next_block_addr() const
{
  return {current_journal_segment->get_segment_id(), written_to + block_size};
}

Journal::roll_journal_segment_ertr::future<>
Journal::roll_journal_segment()
{
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;
    
  return (current_journal_segment ?
	  current_journal_segment->close() :
	  Segment::close_ertr::now()).safe_then(
    [this, old_segment_id] {
      // TODO: pretty sure this needs to be atomic in some sense with
      // making use of the new segment, maybe this bit needs to take
      // the first transaction of the new segment?  Or the segment
      // header should include deltas?
      if (old_segment_id != NULL_SEG_ID) {
	segment_provider->put_segment(old_segment_id);
      }
      return segment_provider->get_segment();
    }).safe_then([this](auto segment) {
      return segment_manager.open(segment);
    }).safe_then([this](auto sref) {
      current_journal_segment = sref;
      written_to = 0;
      return initialize_segment(*current_journal_segment);
    }).handle_error(
      roll_journal_segment_ertr::pass_further{},
      crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
    );
}

Journal::init_ertr::future<> Journal::open_for_write()
{
  return roll_journal_segment();
}

Journal::find_replay_segments_fut Journal::find_replay_segments()
{
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this](auto &&segments) mutable {
      return crimson::do_for_each(
	boost::make_counting_iterator(segment_id_t{0}),
	boost::make_counting_iterator(segment_manager.get_num_segments()),
	[this, &segments](auto i) {
	  return segment_manager.read(paddr_t{i, 0}, block_size
	  ).safe_then([this, &segments, i](bufferptr bptr) mutable {
	    logger().debug("segment {} bptr size {}", i, bptr.length());
	    segment_header_t header;
	    bufferlist bl;
	    bl.push_back(bptr);

	    logger().debug(
	      "find_replay_segments: segment {} block crc {}",
	      i,
	      bl.begin().crc32c(block_size, 0));

	    auto bp = bl.cbegin();
	    try {
	      ::decode(header, bp);
	    } catch (...) {
	      logger().debug(
		"find_replay_segments: segment {} unable to decode "
		"header, skipping",
		i);
	      return find_replay_segments_ertr::now();
	    }
	    segments.emplace_back(std::make_pair(i, std::move(header)));
	    return find_replay_segments_ertr::now();
	  }).handle_error(
	    find_replay_segments_ertr::pass_further{},
	    crimson::ct_error::discard_all{}
	  );
	}).safe_then([this, &segments]() mutable -> find_replay_segments_fut {
	  logger().debug(
	    "find_replay_segments: have {} segments",
	    segments.size());
	  if (segments.empty()) {
	    return crimson::ct_error::input_output_error::make();
	  }
	  std::sort(
	    segments.begin(),
	    segments.end(),
	    [](const auto &lt, const auto &rt) {
	      return lt.second.journal_segment_seq <
		rt.second.journal_segment_seq;
	    });

	  auto replay_from = segments.rbegin()->second.journal_replay_lb;
	  auto from = segments.begin();
	  if (replay_from != P_ADDR_NULL) {
	    from = std::find_if(
	      segments.begin(),
	      segments.end(),
	      [&replay_from](const auto &seg) -> bool {
		return seg.first == replay_from.segment;
	      });
	  } else {
	    replay_from = paddr_t{from->first, block_size};
	  }
	  auto ret = std::vector<paddr_t>(segments.end() - from);
	  std::transform(
	    from, segments.end(), ret.begin(),
	    [this](const auto &p) { return paddr_t{p.first, block_size}; });
	  ret[0] = replay_from;
	  return find_replay_segments_fut(
	    find_replay_segments_ertr::ready_future_marker{},
	    std::move(ret));
	});
    });
}

Journal::replay_ertr::future<>
Journal::replay_segment(
  paddr_t start,
  delta_handler_t &delta_handler)
{
  logger().debug("replay_segment: starting at {}", start);
  return seastar::do_with(
    std::make_tuple(std::move(start), record_header_t()),
    [this, &delta_handler](auto &&in) {
      auto &&[current, header] = in;
      return crimson::do_until(
	[this, &current, &delta_handler, &header] {
	  return segment_manager.read(current, block_size
	  ).safe_then(
	    [this, &current, &header](bufferptr bptr) mutable
	    -> SegmentManager::read_ertr::future<bufferlist> {
	      logger().debug("replay_segment: reading {}", current);
	      bufferlist bl;
	      bl.append(bptr);
	      auto bp = bl.cbegin();
	      try {
		::decode(header, bp);
	      } catch (...) {
		return replay_ertr::make_ready_future<bufferlist>(std::move(bl));
	      }
	      if (header.mdlength > block_size) {
		return segment_manager.read(
		  {current.segment, current.offset + block_size},
		  header.mdlength - block_size).safe_then(
		    [this, bl=std::move(bl)](auto &&bptail) mutable {
		      bl.push_back(bptail);
		      return std::move(bl);
		    });
	      } else {
		return replay_ertr::make_ready_future<bufferlist>(std::move(bl));
	      }
	    }).safe_then([this, &delta_handler, &header](auto bl){
	      auto bp = bl.cbegin();
	      bp += ceph::encoded_sizeof_bounded<record_header_t>();
	      return seastar::do_with(
		std::move(bp),
		[this, &delta_handler, &header](auto &&bp) {
		  return crimson::do_for_each(
		    boost::make_counting_iterator(size_t{0}),
		    boost::make_counting_iterator(header.deltas),
		    [this, &delta_handler, &bp](auto) -> SegmentManager::read_ertr::future<> {
		      delta_info_t dt;
		      try {
			::decode(dt, bp);
		      } catch (...) {
			// need to do other validation, but failures should generally
			// mean we've found the end of the journal
			return crimson::ct_error::input_output_error::make();
		      }
		      return delta_handler(dt);
		    });
		});
	    }).safe_then([&header, &current](){
	      current.offset += header.mdlength + header.dlength;
	      return SegmentManager::read_ertr::make_ready_future<bool>(false);
	    }).handle_error(
	      // TODO: this needs to correctly propogate information about failed
	      // record reads
	      replay_ertr::pass_further{},
	      crimson::ct_error::all_same_way([] {
		logger().debug("replay_segment: error");
		return replay_ertr::make_ready_future<bool>(true);
	      })
	    );
	});
    });
}

Journal::replay_ret Journal::replay(delta_handler_t &&delta_handler)
{
  return seastar::do_with(
    std::make_pair(std::move(delta_handler), std::vector<paddr_t>()),
    [this](auto &&item) mutable -> replay_ret {
      auto &[handler, segments] = item;
      return find_replay_segments(
      ).safe_then([this, &handler, &segments](auto osegments) {
	logger().debug("replay: found {} segments", segments.size());
	segments.swap(osegments);
	return crimson::do_for_each(
	  segments,
	  [this, &handler](auto i) {
	    return replay_segment(i, handler);
	  });
      });
    });
}

}
