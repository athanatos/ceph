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
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

class LBAPin;
using LBAPinRef = std::unique_ptr<LBAPin>;

class LBAPin {
  friend class lba_pin_split_merge;

public:
  virtual loff_t get_length() const = 0;
  virtual paddr_t get_paddr() const = 0;
  virtual laddr_t get_laddr() const = 0;

  virtual ~LBAPin() {}
};

using lba_pin_list_t = std::list<LBAPinRef>;

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  using get_mapping_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_mapping_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mapping_ret get_mappings(
    laddr_t offset, loff_t length) = 0;

  virtual ~LBAManager() {}
};
using LBAManagerRef = std::unique_ptr<LBAManager>;

namespace lba_manager {
LBAManagerRef create_lba_manager(SegmentManager &segment_manager);
}

}
