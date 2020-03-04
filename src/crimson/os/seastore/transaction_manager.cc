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

TransactionManager::TransactionManager(
  SegmentManager &segment_manager, Cache &cache)
  : segment_manager(segment_manager),
    cache(cache),
    lba_manager(lba_manager::create_lba_manager(segment_manager, cache)),
    journal(new Journal(*((JournalSegmentProvider*)nullptr), segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->open_for_write();
}

TransactionManager::read_extent_ret
TransactionManager::read_extents(
  Transaction &t,
  laddr_t offset,
  loff_t length)
{
  std::unique_ptr<lextent_list_t> ret;
  auto &ret_ref = *ret;
  std::unique_ptr<lba_pin_list_t> pin_list;
  auto &pin_list_ref = *pin_list;
  return lba_manager->get_mapping(
    offset, length, t
  ).safe_then([this, &t, &pin_list_ref, &ret_ref](auto pins) {
    pins.swap(pin_list_ref);
    return crimson::do_for_each(
      pin_list_ref.begin(),
      pin_list_ref.end(),
      [this, &t, &ret_ref](auto &pin) {
	// TODO: invert buffer control so that we pass the buffer
	// here into segment_manager to avoid a copy
	return cache.get_extent<LogicalCachedExtent>(
	  t,
	  pin->get_paddr(),
	  pin->get_length()
	).safe_then([this, &pin, &ret_ref](auto ref) mutable {
	  //ref->set_pin(std::move(pin));
	  //ret->push_back(ref);
	  return read_extent_ertr::now();
	});
      });
  }).safe_then([this, ret=std::move(ret), pin_list=std::move(pin_list),
		&t]() mutable {
    return read_extent_ret(
      read_extent_ertr::ready_future_marker{},
      std::move(*ret));
  });
}

TransactionManager::get_mutable_extent_ertr::future<LogicalCachedExtentRef>
TransactionManager::get_mutable_extent(
  Transaction &t,
  laddr_t offset,
  loff_t len)
{
  /* This portion needs to
   * 1) pull relevant extents overlapping offset~len
   * 2) determine whether we're replacing or mutating
   * 3a) replacing: call lba_manager to chop up existing overlapping extents
   * 3g) mutating: just apply mutation
   */
#if 0
  return read_extents(t, {{offset, len}}).safe_then(
    [this, offset, len, &t](auto extent_set) {
      auto extent = cache.duplicate_for_write(extent_set, offset, len);
      t.add_to_write_set(extent);
      return extent;
    });
#endif
  return get_mutable_extent_ertr::make_ready_future<LogicalCachedExtentRef>();
}

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef &&t)
{
  // validate check set
  // invalidate replaced extents stealing pins
  // pass lba_transaction along with current block journal offset and record
  //   to 
  // construct lba transaction thingy [<laddr, pin, len, { ref_diff, optional<paddr> }>...]
  // submit replacement extents to cache with new lba pins and version
  return submit_transaction_ertr::now();
}

TransactionManager::~TransactionManager() {}

}

