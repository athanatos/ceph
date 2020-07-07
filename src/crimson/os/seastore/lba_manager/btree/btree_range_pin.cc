// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/lba_manager/btree/btree_range_pin.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

btree_range_pin_t::~btree_range_pin_t()
{
  assert(!pins == !is_linked());
  assert(!ref);
  if (pins) {
    logger().debug("{}: removing {}", __func__, *this);
    pins->remove_pin(*this, true);
  }
  extent = nullptr;
}

void btree_pin_set_t::remove_pin(btree_range_pin_t &pin, bool do_check_parent)
{
  logger().debug("{}: {}", __func__, pin);
  assert(pin.is_linked());
  
  pins.erase(pin);
  pin.pins = nullptr;
  
  if (do_check_parent) {
    check_parent(pin);
  }
}

btree_range_pin_t *btree_pin_set_t::maybe_get_parent(
  btree_range_pin_t &pin)
{
  auto meta = pin.range;
  meta.depth++;
  auto iter = pins.upper_bound(meta, btree_range_pin_t::meta_cmp_t());
  if (iter == pins.begin()) {
    return nullptr;
  } else {
    --iter;
    if (iter->is_parent_of(pin)) {
      return &*iter;
    } else {
      return nullptr;
    }
  }
}

btree_range_pin_t *btree_pin_set_t::maybe_get_first_child(btree_range_pin_t &pin)
{
  if (pin.range.depth == 0) {
    return nullptr;
  }
  
  auto meta = pin.range;
  meta.depth--;
  
  auto iter = pins.lower_bound(meta, btree_range_pin_t::meta_cmp_t());
  if (iter == pins.end())
    return nullptr;
  if (pin.is_parent_of(*iter)) {
    return &*iter;
  } else {
    return nullptr;
  }
}

void btree_pin_set_t::add_pin(btree_range_pin_t &pin)
{
  assert(!pin.is_linked());
  auto [prev, inserted] = pins.insert(pin);
  if (!inserted) {
    logger().debug("{}: unable to add {}, found {}", __func__, pin, *prev);
    assert(pin.is_linked());
  }
  pin.pins = this;
  if (!pin.is_root()) {
    auto *parent = maybe_get_parent(pin);
    assert(parent);
    if (!parent->has_ref()) {
      logger().debug("{}: acquiring parent {}", __func__, parent);
      parent->acquire_ref();
    } else {
      logger().debug("{}: parent has ref {}", __func__, parent);
    }
  }
  if (maybe_get_first_child(pin) != nullptr) {
    if (!pin.has_ref()) {
      logger().debug("{}: acquiring self {}", __func__, pin);
      pin.acquire_ref();
    } else {
      assert(0 == "impossible");
    }
  }
}

void btree_pin_set_t::retire(btree_range_pin_t &pin)
{
  pin.drop_ref();
  remove_pin(pin, false);
}

void btree_pin_set_t::check_parent(btree_range_pin_t &pin)
{
  auto parent = maybe_get_parent(pin);
  if (parent) {
    logger().debug("{}: releasing parent {}", __func__, *parent);
    release_if_no_children(*parent);
  } else {
    assert(pin.is_root());
  }
}

void btree_pin_set_t::release_if_no_children(btree_range_pin_t &pin)
{
  assert(pin.is_linked());
  if (maybe_get_first_child(pin) == nullptr) {
    pin.drop_ref();
  }
}

}
