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
  auto [all, need] = cache.get_reserve_extents(offset, len);
  /* TODO: need to deal with concurrent access to the same buffer -- probably
     all would include some pending buffers that have to be waited on at the
     end */
  return seastar::do_with(
    std::make_tuple(std::move(all), std::move(need)),
    [this, &t, offset, len](auto &tup) -> read_extent_ret {
      auto &[all, need] = tup;
      return crimson::do_for_each(
	need.begin(),
	need.end(),
	[this, &t](auto &iter) {
	  return lba_manager->get_mapping(
	    iter->get_addr(), iter->get_length()).safe_then(
	      [this, &t, &iter](auto &&pin_ref) {
		iter->set_pin(std::move(pin_ref));
		return seastar::do_with(
		  iter->get_pin().get_mapping(),
		  [this, &t, &iter](auto &mapping) {
		    return crimson::do_for_each(
		      mapping.begin(),
		      mapping.end(),
		      [this, &t, &iter](auto &lpmap) {
			auto &[laddr, paddr, length] = lpmap;
			// TODO: invert buffer control so that we pass the buffer
			// here into segment_manager to avoid a copy
			return segment_manager.read(paddr, length).safe_then(
			  [&iter, &laddr, &length](auto &&bl) {
			    iter->copy_in(bl, laddr, length);
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

