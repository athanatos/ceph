// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/journal.h"

#include "include/denc.h"
#include "include/intarith.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace {
using namespace crimson;
using namespace crimson::os::seastore;

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_id
 * 2) Scan forward from committed_journal_lb to find the most recent
 *    journal_commit_lb record
 * 3) Replay starting at the most recent found journal_commit_lb record
 */
struct segment_header_t {
  Journal::journal_segment_seq_t journal_segment_id;
  segment_id_t physical_segment_id; // debugging

  paddr_t journal_replay_lb;

  DENC(segment_header_t, v, p) {
    denc(v.journal_segment_id, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_replay_lb, p);
  }
};

struct extent_header_t {
  // Fixed portion
  extent_types_t type = extent_types_t::NONE;
  laddr_t laddr;
  segment_off_t length = 0;

  extent_header_t() = default;

  extent_header_t(extent_info_t info)
    : type(info.type), laddr(info.laddr), length(info.bl.length()) {}

  DENC(extent_header_t, v, p) {
    denc(v.type, p);
    denc(v.laddr, p);
    denc(v.length, p);
  }
};

struct record_header_t {
  // Fixed portion
  segment_off_t mdlength;       // block aligned
  segment_off_t dlength;        // block aligned
  journal_seq_t seq;            // current journal seqid
  checksum_t    full_checksum;  // checksum for full record
  size_t deltas;                // number of deltas
  size_t extents;               // number of extents

  DENC(record_header_t, v, p) {
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.seq, p);
    denc(v.full_checksum, p);
    denc(v.deltas, p);
    denc(v.extents, p);
  }
};


}
WRITE_CLASS_DENC_BOUNDED(segment_header_t)
WRITE_CLASS_DENC_BOUNDED(record_header_t)
WRITE_CLASS_DENC_BOUNDED(extent_header_t)

namespace crimson::os::seastore {

Journal::Journal(
  JournalSegmentProvider &segment_provider,
  SegmentManager &segment_manager)
  : max_record_length(segment_manager.get_segment_size() /* TODO */),
    block_size(segment_manager.get_block_size()),
    segment_provider(segment_provider),
    segment_manager(segment_manager) {}


Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment)
{
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;
  auto header = segment_header_t{
    current_journal_segment_id++,
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
    ::encode(extent_header_t{i}, metadatabl);
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
    metadata += ceph::encoded_sizeof_bounded<extent_header_t>();
  }
  metadata = p2roundup(metadata, (size_t)block_size);
  return std::make_pair(metadata, data);
}

bool Journal::needs_roll(segment_off_t length) const {
  return length + written_to >
    current_journal_segment->get_write_capacity();
}

Journal::roll_journal_segment_ertr::future<>
Journal::roll_journal_segment()
{
  return current_journal_segment->close().safe_then(
    [this, old_segment_id = current_journal_segment->get_segment_id()] {
      // TODO: pretty sure this needs to be atomic in some sense with
      // making use of the new segment, maybe this bit needs to take
      // the first transaction of the new segment?  Or the segment
      // header should include deltas?
      segment_provider.put_segment(old_segment_id);
      return segment_provider.get_segment();
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

}
