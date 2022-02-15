// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "include/intarith.h"
#include "crimson/os/seastore/circular_bounded_journal.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const CBJournal::cbj_header_t &header)
{
  return out << "cbj_header_t(magin=" << header.magic
	     << ", uuid=" << header.uuid
	     << ", block_size=" << header.block_size
	     << ", size=" << header.size
	     << ", used_size=" << header.used_size
	     << ", error=" << header.error
	     << ", start_offset=" << header.start_offset
	     << ", applied_to="<< header.applied_to
	     << ", last_committed_record_base="<< header.last_committed_record_base
	     << ", written_to=" << header.written_to
	     << ", flsg=" << header.flag
	     << ", csum_type=" << header.csum_type
	     << ", csum=" << header.csum
	     << ", start=" << header.start
	     << ", end=" << header.end
             << ")";
}


CBJournal::CBJournal(NVMeBlockDevice* device, const std::string path)
  : device(device), path(path) {}

CBJournal::mkfs_ret
CBJournal::mkfs(mkfs_config_t& config)
{
  return _open_device(path
  ).safe_then([this, &config]() {
    rbm_abs_addr start_addr = convert_paddr_to_abs_addr(
      config.start);
    return read_super(start_addr
    ).safe_then([](auto super) {
      return mkfs_ertr::now();
    }).handle_error(
      crimson::ct_error::enoent::handle([this, &config, start_addr] (auto) 
	-> mkfs_ret {
	ceph::bufferlist bl;
	CBJournal::cbj_header_t head;
	try {
	  head.magic = CBJOURNAL_MAGIC;
	  head.uuid = uuid_d(); // TODO
	  head.block_size = device->get_block_size();
	  rbm_abs_addr end_addr = convert_paddr_to_abs_addr(
	    config.end);
	  head.size = end_addr - start_addr
	    - device->get_block_size();
	  head.used_size = 0;
	  head.error = 0;
	  head.start_offset = device->get_block_size();
	  head.last_committed_record_base = 0;
	  head.written_to = head.start_offset;
	  head.applied_to = 0;
	  head.flag = 0;
	  head.csum_type = 0;
	  head.csum = 0;
	  head.cur_segment_seq = 0;
	  head.start = start_addr;
	  head.end = end_addr;
	  head.device_id = config.device_id;
	  encode(head, bl);
	} catch (ceph::buffer::error &e) {
	  logger().debug("unable to encode super block from underlying deivce");
	  return mkfs_ertr::now();
	}
	size = head.size;
	block_size = config.block_size;
	logger().debug(
	  "initialize header block in CBJournal, length {}",
	  bl.length());
	return device_write_bl(start_addr, bl
	).handle_error(
	  mkfs_ertr::pass_further{},
	  crimson::ct_error::assert_all{
	  "Invalid error device_write during CBJournal::mkfs"
	});
      }),
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error read_super in CBJournal::mkfs"
      }
    );
  }).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error _open_device in CBJournal::mkfs"
  }).finally([this] {
    if (device) {
      return device->close();
    } else {
      return seastar::now();
    }
  });
}

CBJournal::open_for_write_ertr::future<> CBJournal::_open_device(
        const std::string path)
{
  ceph_assert(device);
  return device->open(path, seastar::open_flags::rw
  ).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error device->open"
    }
  );
}

ceph::bufferlist CBJournal::encode_super()
{
  bufferlist bl;
  encode(header, bl);
  return bl;
}

CBJournal::open_for_write_ret CBJournal::open_for_write()
{
  return open_for_write(CBJOURNAL_START_ADDRESS);
}

CBJournal::close_ertr::future<> CBJournal::close()
{
  return sync_super(
  ).safe_then([this]() -> close_ertr::future<> {
    return device->close();
  }).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error sync_super"
    }
  );
}

CBJournal::open_for_write_ret CBJournal::open_for_write(rbm_abs_addr start)
{
  return _open_device(path
  ).safe_then([this, start]() {
    return read_super(start
    ).handle_error(
      open_for_write_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error read_super"
    }).safe_then([this, start](auto p) mutable {
      auto &[head, bl] = *p;
      header = head;
      written_to = header.written_to;
      last_committed_record_base = header.last_committed_record_base;
      applied_to = header.applied_to;
      cur_segment_seq = header.cur_segment_seq;
      block_size = header.block_size;
      used_size = header.used_size;
      size = header.size;
      this->start = start;
      logger().debug(" super : {} ", header);
      paddr_t paddr = convert_abs_addr_to_paddr(
	written_to,
	header.device_id);
      return open_for_write_ret(
	open_for_write_ertr::ready_future_marker{},
	journal_seq_t{
	  cur_segment_seq,
	  paddr
	});
    });
  }).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error _open_device"
  });
}

CBJournal::write_ertr::future<> CBJournal::append_record(
  ceph::bufferlist bl,
  rbm_abs_addr addr)
{
  bufferlist to_write;
  if (addr + bl.length() <= header.end) {
    to_write = bl;
  } else {
    to_write.substr_of(bl, 0, header.end - addr);
  }
  logger().debug(
    "append_record: offset {}, length {}",
    addr,
    to_write.length());

  auto bptr = bufferptr(ceph::buffer::create_page_aligned(to_write.length()));
  auto iter = to_write.cbegin();
  iter.copy(to_write.length(), bptr.c_str());
  return device->write(addr, bptr
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error device->write" }
  ).safe_then([this, bl=std::move(bl), length=to_write.length()] {
    if (bl.length() == length) {
      // complete
      return write_ertr::now();
    } else {
      // write remaining data---in this case,
      // data is splited into two parts before due to the end of CBJournal.
      // the following code is to write the second part
      auto next = get_start_addr();
      bufferlist next_write;
      next_write.substr_of(bl, length, bl.length() - length);
      auto bp = bufferptr(
	  ceph::buffer::create_page_aligned(next_write.length()));
      auto iter = next_write.cbegin();
      iter.copy(next_write.length(), bp.c_str());
      return device->write(next, bp
      ).handle_error(
	write_ertr::pass_further{},
	crimson::ct_error::assert_all{ "Invalid error device->write" }
      ).safe_then([] {
	return write_ertr::now();
      });
    }
  });
}

CBJournal::submit_record_ret CBJournal::submit_record(
  record_t &&record,
  OrderingHandle &handle)
{
  assert(write_pipeline);
  auto r_size = record_group_size_t(record.size, block_size);
  auto encoded_size = r_size.get_encoded_length();
  if (encoded_size > get_available_size()) {
    logger().error(
      "CBJournal::submit_record: record size {}, but available size {}",
      encoded_size,
      get_available_size()
      );
    return crimson::ct_error::erange::make();
  }

  journal_seq_t j_seq {
    cur_segment_seq++,
    convert_abs_addr_to_paddr(
      written_to,
      header.device_id)};
  ceph::bufferlist to_write = encode_record(
    std::move(record), device->get_block_size(),
    j_seq, 0);
  if (written_to +
      ceph::encoded_sizeof_bounded<record_group_header_t>() > header.end) {
    // not enough space between written_to and the end of journal
    used_size += header.end - written_to;
    written_to = get_start_addr();
    if (encoded_size > get_available_size()) {
      return crimson::ct_error::erange::make();
    }
  }
  auto target = written_to;
  if (written_to + to_write.length() >= header.end) {
    written_to = get_start_addr() +
      (to_write.length() - (header.end - written_to));
  } else {
    written_to += to_write.length();
  }
  logger().debug(
    "submit_record: mdlength {}, dlength {}, target {}",
    r_size.get_mdlength(),
    r_size.dlength,
    target);

  auto write_result = write_result_t{
    j_seq,
    (seastore_off_t)to_write.length()
  };
  auto write_fut = append_record(to_write, target);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut
    ).handle_error(
      write_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in CBJournal::append_record"
      }
    );
  }).safe_then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).safe_then([this, target,
    length=to_write.length(),
    &write_result,
    &r_size] {
    logger().debug(
      "append_record: commit target {} used_size {} written length {}",
      target, used_size, length);

    last_committed_record_base = target;
    used_size += length;
    paddr_t paddr = convert_abs_addr_to_paddr(
      target + r_size.get_mdlength(),
      header.device_id);
    auto submit_result = record_locator_t{
      paddr,
      write_result
    };
    return submit_result;
  });
}

CBJournal::write_ertr::future<> CBJournal::device_write_bl(
    rbm_abs_addr offset, bufferlist &bl)
{
  auto length = bl.length();
  if (offset + length > header.end) {
    return crimson::ct_error::erange::make();
  }
  logger().debug(
    "overwrite in CBJournal, offset {}, length {}",
    offset,
    length);
  auto write_length = length < block_size ? block_size : length;
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(write_length));
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bptr.c_str());
  return device->write(offset, bptr
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error device->write" }
  ).safe_then([] {
    return write_ertr::now();
  });
}

CBJournal::read_super_ret CBJournal::read_super(rbm_abs_addr start)
{
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(block_size));
  return device->read(start, bptr
  ).safe_then([start, bptr]() mutable
    -> read_super_ret {
    logger().debug("read_super: reading {}", start);
    bufferlist bl;
    bl.append(bptr);
    auto bp = bl.cbegin();
    cbj_header_t cbj_header;
    try {
      decode(cbj_header, bp);
    } catch (ceph::buffer::error &e) {
      logger().debug("read_super: unable to read super block");
      return crimson::ct_error::enoent::make();
    }
    return read_super_ret(
      read_super_ertr::ready_future_marker{},
      std::make_pair(cbj_header, bl)
    );
  });
}

CBJournal::read_record_ret CBJournal::return_record(record_group_header_t& header, bufferlist bl)
{
  bufferlist md_bl, data_bl;
  md_bl.substr_of(bl, 0, block_size);
  data_bl.substr_of(bl, header.mdlength, header.dlength);
  if (validate_records_metadata(md_bl) &&
      validate_records_data(header, data_bl)) {
    return read_record_ret(
      read_record_ertr::ready_future_marker{},
      std::make_pair(header, std::move(bl)));
  } else {
    logger().debug("invalid matadata");
    return read_record_ret(
      read_record_ertr::ready_future_marker{},
      std::nullopt);
  }
}

Journal::replay_ret CBJournal::replay(
  delta_handler_t &&delta_handler)
{
  /*
   * read records from last applied record prior to written_to, and replay
   */
  if (last_committed_record_base == applied_to) {
    return replay_ertr::now();
  }
  return seastar::do_with(
    rbm_abs_addr(applied_to),
    std::move(delta_handler),
    [this](auto &cursor_addr, auto &d_handler) {
    return crimson::repeat(
      [this, &cursor_addr, &d_handler]() mutable
      -> replay_ertr::future<seastar::stop_iteration> {
      paddr_t cursor_paddr = convert_abs_addr_to_paddr(
	cursor_addr,
	header.device_id);
      return read_record(cursor_paddr
      ).safe_then([this, &cursor_addr, &d_handler] (auto ret) {
	auto [r_header, bl] = *ret;
	if (cursor_addr == applied_to) {
	  cursor_addr += bl.length();
	  return replay_ertr::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::no);
	}
	bufferlist mdbuf;
	mdbuf.substr_of(bl, 0, r_header.mdlength);
	paddr_t record_block_base = paddr_t::make_blk_paddr(
	  header.device_id, cursor_addr);
	auto maybe_record_deltas_list = try_decode_deltas(
	  r_header, mdbuf, record_block_base);
	if (!maybe_record_deltas_list) {
	  logger().debug("unable to decode deltas for record {} at {}",
	    r_header, record_block_base);
	  return replay_ertr::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::yes);
	}
	logger().debug(" record_group_header_t: {}, cursor_addr: {} ",
	  r_header, cursor_addr);
	auto write_result = write_result_t{
	  r_header.committed_to,
	  (seastore_off_t)bl.length()
	};
	cursor_addr += bl.length();
	return seastar::do_with(
	  std::move(*maybe_record_deltas_list),
	  [write_result,
	  this,
	  &d_handler,
	  &cursor_addr](auto& record_deltas_list) {
	  return crimson::do_for_each(
	    record_deltas_list,
	    [write_result,
	    &d_handler](record_deltas_t& record_deltas) {
	    auto locator = record_locator_t{
	      record_deltas.record_block_base,
	      write_result
	    };
	    logger().debug("processing {} deltas at block_base {}",
		  record_deltas.deltas.size(),
		  locator);
	    return crimson::do_for_each(
	      record_deltas.deltas,
	      [locator,
	       &d_handler](delta_info_t& delta){
	      return d_handler(locator, delta);
	    });
	  }).safe_then([this, &cursor_addr]() {
	    if (cursor_addr >= header.end) {
	      cursor_addr = get_start_addr();
	    }
	    if (cursor_addr == last_committed_record_base) {
	      return replay_ertr::make_ready_future<
		seastar::stop_iteration>(seastar::stop_iteration::yes);
	    }
	    return replay_ertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::no);
	  });
	});
      });
    });
  });
}

CBJournal::read_record_ret CBJournal::read_record(paddr_t off)
{
  rbm_abs_addr offset = convert_paddr_to_abs_addr(
    off);
  rbm_abs_addr addr = offset;
  auto read_length = block_size;
  if (addr + block_size > header.end) {
    addr = get_start_addr();
    read_length = header.end - offset;
  }
  logger().debug("read_record: reading record from abs addr {} read length {}",
      addr, read_length);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(read_length));
  bptr.zero();
  return device->read(addr, bptr
  ).safe_then(
    [this, addr, read_length, bptr]() mutable
    -> read_record_ret {
      record_group_header_t h;
      bufferlist bl;
      bl.append(bptr);
      auto bp = bl.cbegin();
      try {
	decode(h, bp);
      } catch (ceph::buffer::error &e) {
	return read_record_ret(
	  read_record_ertr::ready_future_marker{},
	  std::nullopt);
      }
      /*
       * |          journal          |
       *        | record 1 header |  | record 1 data
       *  record 1 data  (remaining) |
       *
       *        <---- 1 block ----><--
       * -- 2 block --->
       *
       *  If record has logner than read_length and its data is located across
       *  the end of journal and the begining of journal, we need three reads
       *  ---reads of header, other remaining data before the end, and
       *  the other remaining data from the begining.
       *
       */
      if (h.mdlength + h.dlength > read_length) {
	rbm_abs_addr next_read_addr = addr + read_length;
	auto next_read = h.mdlength + h.dlength - read_length;
	logger().debug(" next_read_addr {}, next_read_length {} ",
	    next_read_addr, next_read);
	if (header.end < next_read_addr + next_read) {
	  // In this case, need two more reads.
	  // The first is to read remain bytes to the end of cbjournal
	  // The second is to read the data at the begining of cbjournal
	  next_read = header.end - (addr + read_length);
	}
	logger().debug("read_entry: additional reading addr {} length {}",
			next_read_addr,
			next_read);
	auto next_bptr = bufferptr(ceph::buffer::create_page_aligned(next_read));
	next_bptr.zero();
	return device->read(
	  next_read_addr,
	  next_bptr
	).safe_then(
	  [this, h=h, next_bptr=std::move(next_bptr), bl=std::move(bl)]() mutable {
	    bl.append(next_bptr);
	    if (h.mdlength + h.dlength == bl.length()) {
	      logger().debug("read_record: record length {} done", bl.length());
	      return return_record(h, bl);
	    }
	    // need one more read
	    auto next_read_addr = get_start_addr();
	    auto last_bptr = bufferptr(ceph::buffer::create_page_aligned(
		  h.mdlength + h.dlength - bl.length()));
	    logger().debug("read_record: last additional reading addr {} length {}",
			    next_read_addr,
			    h.mdlength + h.dlength - bl.length());
	    return device->read(
	      next_read_addr,
	      last_bptr
	    ).safe_then(
	      [this, h=h, last_bptr=std::move(last_bptr), bl=std::move(bl)]() mutable {
		bl.append(last_bptr);
		logger().debug("read_record: complte size {}", bl.length());
		return return_record(h, bl);
	      });
	});
      } else {
	logger().debug("read_record: complte size {}", bl.length());
	return return_record(h, bl);
      }
    });
}

CBJournal::write_ertr::future<>
CBJournal::sync_super()
{
  header.used_size = used_size;
  header.size = size;
  header.block_size = block_size;
  header.applied_to = applied_to;
  header.last_committed_record_base = last_committed_record_base;
  header.written_to = written_to;
  header.cur_segment_seq = cur_segment_seq;
  ceph::bufferlist bl;
  try {
    bl = encode_super();
  } catch (ceph::buffer::error &e) {
    logger().debug("unable to encode super block from underlying deivce");
    return crimson::ct_error::input_output_error::make();
  }
  logger().debug(
    "sync header of CBJournal, length {}",
    bl.length());
  return device_write_bl(start, bl);
}

}
