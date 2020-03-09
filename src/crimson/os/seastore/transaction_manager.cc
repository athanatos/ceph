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

TransactionManager::inc_ref_ertr::future<> TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref) {
  return lba_manager->incref_extent(
    t,
    ref->get_pin()).handle_error(
      inc_ref_ertr::pass_further{},
      ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "unhandled error, TODO");
      }));
}

TransactionManager::inc_ref_ertr::future<> TransactionManager::inc_ref(
  Transaction &t,
  laddr_t offset,
  loff_t len) {
  std::unique_ptr<lba_pin_list_t> pins;
  lba_pin_list_t &pins_ref = *pins;
  return lba_manager->get_mapping(
    t,
    offset,
    len
  ).safe_then([this, &t, &pins_ref](auto pins) {
    pins_ref.swap(pins);
    return crimson::do_for_each(
      pins_ref.begin(),
      pins_ref.end(),
      [this, &t](auto &pin) {
	return lba_manager->incref_extent(
	  t,
	  *pin);
      });
  }).safe_then([this, pins=std::move(pins)] {
    return inc_ref_ertr::now();
  });
}

TransactionManager::dec_ref_ertr::future<> TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref) {
  return lba_manager->decref_extent(
    t,
    ref->get_pin()).handle_error(
      dec_ref_ertr::pass_further{},
      ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "unhandled error, TODO");
      })).safe_then([](auto) {});
}

TransactionManager::dec_ref_ertr::future<> TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset,
  loff_t len) {
  std::unique_ptr<lba_pin_list_t> pins;
  lba_pin_list_t &pins_ref = *pins;
  return lba_manager->get_mapping(
    t,
    offset,
    len
  ).safe_then([this, &t, &pins_ref](auto pins) {
    pins_ref.swap(pins);
    return crimson::do_for_each(
      pins_ref.begin(),
      pins_ref.end(),
      [this, &t](auto &pin) {
	return lba_manager->decref_extent(
	  t,
	  *pin).safe_then([](auto) {});
      });
  }).safe_then([this, pins=std::move(pins)] {
    return dec_ref_ertr::now();
  });
}

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef t)
{
  auto record = cache.try_construct_record(*t);
  if (!record) {
    return crimson::ct_error::eagain::make();
  }

  return journal->submit_record(std::move(*record)).safe_then(
    [this, t=std::move(t)](paddr_t addr) {
      cache.complete_commit(*t, addr);
    },
    submit_transaction_ertr::pass_further{},
    crimson::ct_error::all_same_way([](auto e) {
      ceph_assert(0 == "Hit error submitting to journal");
    }));
}

TransactionManager::~TransactionManager() {}

}

