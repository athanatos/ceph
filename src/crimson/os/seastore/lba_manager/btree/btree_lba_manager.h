// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore::lba_manager::btree {

class BtreeLBAManager : public LBAManager {
  SegmentManager &segment_manager;
public:
  BtreeLBAManager(SegmentManager &segment_manager);

  get_mapping_ret get_mappings(laddr_t offset, loff_t length);
};
  
}
