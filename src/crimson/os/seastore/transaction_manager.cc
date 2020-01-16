// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

Transaction::Transaction(paddr_t start) : start(start) {}

TransactionManager::TransactionManager(SegmentManager &segment_manager)
  : segment_manager(segment_manager)
{}

TransactionManager::init_ertr::future<> TransactionManager::init()
{
  return init_ertr::now();
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

};

