// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"

namespace crimson::os::seastore::onode {

class FLTreeOnodeManager : public crimson::os::seastore::OnodeManager {
public:
  open_ertr::future<OnodeRef> get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) final {
    return open_ertr::make_ready_future<OnodeRef>();
  }
  open_ertr::future<std::vector<OnodeRef>> get_or_create_onodes(
    Transaction &trans,
    const std::vector<ghobject_t> &hoids) final {
    return open_ertr::make_ready_future<std::vector<OnodeRef>>();
  }

  write_ertr::future<> write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) final {
    return write_ertr::now();
  }

  ~FLTreeOnodeManager();
};

}
