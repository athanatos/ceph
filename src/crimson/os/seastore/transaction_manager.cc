// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

TransactionManager::TransactionManager(SegmentManager &segment_manager)
  : segment_manager(segment_manager),
    lba_manager(lba_manager::create_lba_manager(segment_manager)),
    journal(new Journal(*((JournalSegmentProvider*)nullptr), segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->open_for_write();
}

TransactionManager::read_extent_ret
TransactionManager::read_extents(
  Transaction &t,
  const extent_list_t &extents)
{
  auto [all, remaining] = t.get_extents(extents);
  auto [from_cache, need, pending] = cache.get_reserve_extents(remaining);
  all.merge(std::move(from_cache));
  return seastar::do_with(
    std::make_tuple(std::move(need), ExtentSet()),
    [this](auto &tup) -> read_extent_ret {
      auto &[need, read] = tup;
      return crimson::do_for_each(
	need.begin(),
	need.end(),
	[this, &read](auto &iter) {
	  auto &[offset, length] = iter;
	  return lba_manager->get_mappings(offset, length).safe_then(
	    [this, &read](auto &&pin_refs) {
		return seastar::do_with(
		  std::move(pin_refs),
		  [this, &read](auto &lba_pins) {
		    return crimson::do_for_each(
		      lba_pins.begin(),
		      lba_pins.end(),
		      [this, &read](auto &pin) {
			// TODO: invert buffer control so that we pass the buffer
			// here into segment_manager to avoid a copy
			return segment_manager.read(
			  pin->get_paddr(),
			  pin->get_length()
			).safe_then([this, &read, &pin](auto bl) mutable {
			  auto eref = cache.get_extent_buffer(
			    pin->get_laddr(),
			    pin->get_length());
			  eref->set_pin(std::move(pin));
			  eref->copy_in(bl, pin->get_laddr(), pin->get_length());
			  read.insert(eref);
			  return read_extent_ertr::now();
			});
		      });
		  });
	      });
	}).safe_then([this, &read]() mutable {
	  // offer all to transaction
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(read));
	});
    }).safe_then([this, &t, all=std::move(all), pending=std::move(pending)](
		   auto &&read) {
      return cache.await_pending(pending).safe_then(
	[this, &t, all=std::move(all), read=std::move(read)](
	  auto &&pending_extents) mutable {
	  t.add_to_read_set(pending_extents);
	  t.add_to_read_set(read);
	  all.merge(std::move(pending_extents));
	  all.merge(std::move(read));
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(all));
	});
    });
}

TransactionManager::get_mutable_extent_ertr::future<CachedExtentRef>
TransactionManager::get_mutable_extent(
  Transaction &t,
  laddr_t offset,
  loff_t len)
{
  return read_extents(t, {{offset, len}}).safe_then(
    [this, offset, len, &t](auto extent_set) {
      auto extent = cache.duplicate_for_write(extent_set, offset, len);
      t.add_to_write_set(extent);
      return extent;
    });
}

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef &&t)
{
  // grab pins for all pending extents in the write set
  // validate read set
  // invalidate replaced extents stealing pins
  // construct lba transaction thingy [<laddr, pin, len, { ref_diff, optional<paddr> }>...]
  // submit replacement extents to cache with new lba pins and version
  return submit_transaction_ertr::now();
}

TransactionManager::~TransactionManager() {}

}

