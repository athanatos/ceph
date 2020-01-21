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

Transaction::Transaction(paddr_t start) : start(start) {}

TransactionManager::TransactionManager(SegmentManager &segment_manager)
  : journal(new Journal(segment_manager))
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return journal->init_write(SegmentRef(), 0 /* TODO */);
}

TransactionManager::read_ertr::future<> TransactionManager::add_delta(
  TransactionRef &trans,
  paddr_t paddr,
  laddr_t laddr,
  ceph::bufferlist delta,
  ceph::bufferlist bl)
{
  return read_ertr::now();
}

paddr_t TransactionManager::add_block(
  TransactionRef &trans,
  laddr_t laddr,
  ceph::bufferlist bl)
{
  return {0, 0};
}

TransactionManager::~TransactionManager() {}

}

