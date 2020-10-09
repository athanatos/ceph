// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <seastar/core/sleep.hh>

#include "include/buffer_raw.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

namespace crimson::os::seastore::onode {

class DummySuper final: public Super {
 public:
  DummySuper(Transaction& t, RootNodeTracker& tracker, laddr_t* p_root_laddr)
      : Super(t, tracker), p_root_laddr{p_root_laddr} {}
  ~DummySuper() override = default;
 protected:
  laddr_t get_root_laddr() const override { return *p_root_laddr; }
  void write_root_laddr(context_t, laddr_t addr) override { *p_root_laddr = addr; }
  laddr_t* p_root_laddr;
};

class DummyNodeExtent final: public NodeExtent {
 public:
  DummyNodeExtent(ceph::bufferptr &&ptr) : NodeExtent(std::move(ptr)) {
    state = extent_state_t::INITIAL_WRITE_PENDING;
  }
  ~DummyNodeExtent() override = default;
 protected:
  NodeExtentRef mutate(context_t) override {
    assert(false && "impossible path"); }
  CachedExtentRef duplicate_for_write() override {
    assert(false && "impossible path"); }
  extent_types_t get_type() const override {
    assert(false && "impossible path"); }
  ceph::bufferlist get_delta() override {
    assert(false && "impossible path"); }
  void apply_delta(const ceph::bufferlist&) override {
    assert(false && "impossible path"); }
};

template <bool SYNC>
class DummyNodeExtentManager final: public NodeExtentManager {
  static constexpr size_t ALIGNMENT = 4096;
 public:
  ~DummyNodeExtentManager() override = default;
 protected:
  bool is_read_isolated() const override { return false; }

  tm_future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr, extent_len_t len) override {
    if constexpr (SYNC) {
      return read_extent_sync(t, addr, len);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, addr, len] {
        return read_extent_sync(t, addr, len);
      });
    }
  }

  tm_future<NodeExtentRef> alloc_extent(
      Transaction& t, extent_len_t len) override {
    if constexpr (SYNC) {
      return alloc_extent_sync(t, len);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, len] {
        return alloc_extent_sync(t, len);
      });
    }
  }

  tm_future<Super::URef> get_super(
      Transaction& t, RootNodeTracker& tracker) override {
    if constexpr (SYNC) {
      return get_super_sync(t, tracker);
    } else {
      using namespace std::chrono_literals;
      return seastar::sleep(1us).then([this, &t, &tracker] {
        return get_super_sync(t, tracker);
      });
    }
  }

 private:
  tm_future<NodeExtentRef> read_extent_sync(
      Transaction& t, laddr_t addr, extent_len_t len) {
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    assert(iter->second->get_length() == len);
    return tm_ertr::make_ready_future<NodeExtentRef>(iter->second);
  }

  tm_future<NodeExtentRef> alloc_extent_sync(
      Transaction& t, extent_len_t len) {
    assert(len % ALIGNMENT == 0);
    auto r = ceph::buffer::create_aligned(len, ALIGNMENT);
    auto addr = reinterpret_cast<laddr_t>(r->get_data());
    auto bp = ceph::bufferptr(std::move(r));
    auto extent = Ref<DummyNodeExtent>(new DummyNodeExtent(std::move(bp)));
    extent->set_laddr(addr);
    assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
    allocate_map.insert({extent->get_laddr(), extent});
    return tm_ertr::make_ready_future<NodeExtentRef>(extent);
  }

  tm_future<Super::URef> get_super_sync(
      Transaction& t, RootNodeTracker& tracker) {
    return tm_ertr::make_ready_future<Super::URef>(
        Super::URef(new DummySuper(t, tracker, &root_laddr)));
  }

  std::map<laddr_t, Ref<DummyNodeExtent>> allocate_map;
  laddr_t root_laddr = L_ADDR_NULL;
};

}
