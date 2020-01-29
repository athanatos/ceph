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
    lba_manager(*((LBAManager*)nullptr)),
    journal(new Journal(*((JournalSegmentProvider*)nullptr), segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->open_for_write();
}


TransactionManager::mutate_ertr::future<>
TransactionManager::mutate(
  Transaction &t,
  extent_types_t type,
  laddr_t offset,
  loff_t len,
  buffer_mut_f &&f) {
  // read offset~len from cache
  // read offset~len from segment_manager
  // deltabl (type) = f(blocks)
  return mutate_ertr::now();
}
  

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef &&t)
{
  return submit_transaction_ertr::now();
}

TransactionManager::~TransactionManager() {}

}

