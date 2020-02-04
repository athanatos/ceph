// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::BtreeLBAManager(
  SegmentManager &segment_manager)
  : segment_manager(segment_manager) {}


BtreeLBAManager::get_mapping_ret BtreeLBAManager::get_mappings(
  laddr_t offset, loff_t length)
{
  return get_mapping_ret(
    get_mapping_ertr::ready_future_marker{},
    lba_pin_list_t());
}

}


