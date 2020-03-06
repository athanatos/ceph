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

