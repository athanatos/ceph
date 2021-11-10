// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

std::ostream &LBAInternalNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

void LBAInternalNode::resolve_relative_addrs(paddr_t base)
{
  for (auto i: *this) {
    if (is_relative(i->get_val())) {
      auto updated = base.add_relative(i->get_val());
      logger().debug(
	"LBAInternalNode::resolve_relative_addrs {} -> {}",
	i->get_val(),
	updated);
      i->set_val(updated);
    }
  }
}

std::ostream &LBALeafNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

void LBALeafNode::resolve_relative_addrs(paddr_t base)
{
  for (auto i: *this) {
    if (is_relative(i->get_val().paddr)) {
      auto val = i->get_val();
      val.paddr = base.add_relative(val.paddr);
      logger().debug(
	"LBALeafNode::resolve_relative_addrs {} -> {}",
	i->get_val().paddr,
	val.paddr);
      i->set_val(val);
    }
  }
}

}
