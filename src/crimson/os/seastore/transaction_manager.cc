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
    lba_manager(lba_manager::create_lba_manager()),
    journal(new Journal(*((JournalSegmentProvider*)nullptr), segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->open_for_write();
}

TransactionManager::read_extent_ret
TransactionManager::read_extent(
  Transaction &t,
  laddr_t offset,
  loff_t len)
{
  auto [all, need, pending] = cache.get_reserve_extents(offset, len);
  //t.add_read_set(std::move(all));
  return seastar::do_with(
    std::make_tuple(std::move(all), std::move(need), ExtentSet()),
    [this, &t, offset, len](auto &tup) -> read_extent_ret {
      auto &[all, need, read] = tup;
      return crimson::do_for_each(
	need.begin(),
	need.end(),
	[this, &t, &all, &read](auto &iter) {
	  auto &[offset, length] = iter;
	  return lba_manager->get_mappings(offset, length).safe_then(
	    [this, &t, &all, &read](auto &&pin_refs) {
		return seastar::do_with(
		  std::move(pin_refs),
		  [this, &t, &all, &read](auto &lba_pins) {
		    return crimson::do_for_each(
		      lba_pins.begin(),
		      lba_pins.end(),
		      [this, &t, &all, &read](auto &pin) {
			// TODO: invert buffer control so that we pass the buffer
			// here into segment_manager to avoid a copy
			return segment_manager.read(
			  pin->get_paddr(),
			  pin->get_length()
			).safe_then([this, &t, &all, &read, &pin](auto bl) mutable {
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
	}).safe_then([this, &t, offset, len, &all]() mutable {
	  // offer all to transaction
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(all));
	});
    }).safe_then([this, &t, pending=std::move(pending)](auto &&extent_set) {
      return cache.await_pending(pending).safe_then(
	[this, &t, extent_set=std::move(extent_set)](
	  auto &&pending_extents) mutable {
	  extent_set.merge(std::move(pending_extents));
	  return read_extent_ret(
	    read_extent_ertr::ready_future_marker{},
	    std::move(extent_set));
	});
    });
}

TransactionManager::get_mutable_extent_ertr::future<CachedExtentRef>
TransactionManager::get_mutable_extent(
  Transaction &t,
  laddr_t offset,
  loff_t len)
{
  return get_mutable_extent_ertr::make_ready_future<CachedExtentRef>(nullptr);
}

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef &&t)
{
  return submit_transaction_ertr::now();
}

TransactionManager::~TransactionManager() {}

}

