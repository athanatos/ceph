// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

struct root_block_t {
  bufferlist lba_root;

  DENC(root_block_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.lba_root, p);
    DENC_FINISH(p);
  }
};

struct RootBlock : CachedExtent {
  using Ref = TCachedExtentRef<RootBlock>;

  root_block_t root;

  template <typename... T>
  RootBlock(T&&... t) : CachedExtent(std::forward<T>(t)...) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new RootBlock(*this));
  };

  void on_written(paddr_t record_block_offset) final {
  }

  extent_types_t get_type() final {
    return extent_types_t::ROOT;
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  void apply_delta(ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final {
    auto biter = get_bptr().cbegin();
    root.decode(biter);
    return complete_load_ertr::now();
  }
};
using RootBlockRef = RootBlock::Ref;

}

WRITE_CLASS_DENC(crimson::os::seastore::root_block_t)
