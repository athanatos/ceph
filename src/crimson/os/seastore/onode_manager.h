// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class Onode: public boost::intrusive_ref_counter<
  Onode,
  boost::thread_unsafe_counter>{
public:
};
using OnodeRef = boost::intrusive_ptr<Onode>;


class OnodeManager {
public:
  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::enoent>;
  virtual open_ertr::future<OnodeRef> get_onode(
    const coll_t &cid,
    const ghobject_t &hoid) {
    return open_ertr::make_ready_future<OnodeRef>();
  }

  virtual ~OnodeManager() {}
};
using OnodeManagerRef = std::unique_ptr<OnodeManager>;
  
namespace onode_manager {

OnodeManagerRef create_ephemeral() {
  return OnodeManagerRef();
}

}

}
