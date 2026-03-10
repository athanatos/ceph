// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs)
{
  if (rhs.is_end()) {
    return out << "LBAMapping(END)";
  }
  out << "LBAMapping(" << rhs.get_key()
      << "~0x" << std::hex << rhs.get_length();
  if (rhs.is_complete()) {
    out << std::dec
	<< "->" << rhs.get_val();
  } else {
    out << std::dec << "->" << rhs.indirect_cursor->get_pladdr();
  }
  if (rhs.is_complete_indirect()) {
    out << ",indirect(" << rhs.get_intermediate_base()
        << "~0x" << std::hex << rhs.get_intermediate_length()
        << "@0x" << rhs.get_intermediate_offset() << std::dec
        << ")";
  }
  out << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const lba_mapping_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << i;
    first = false;
  }
  return out << ']';
}

using lba::LBALeafNode;

get_child_ret_t<LBALeafNode, LogicalChildNode>
LBAMapping::get_logical_extent(Transaction &t) const
{
  return direct_cursor->get_logical_extent(t);
}

bool LBAMapping::is_stable() const {
  return direct_cursor->is_stable();

}

bool LBAMapping::is_data_stable() const {
  return direct_cursor->is_data_stable();

}

base_iertr::future<LBAMapping> LBAMapping::next()
{
  LOG_PREFIX(LBAMapping::next);
  SUBDEBUG(seastore_lba, "{}", *this);
  auto cursor = get_effective_cursor_ref();
  co_await cursor->next();
  if (cursor->is_indirect()) {
    co_return LBAMapping::create_indirect(nullptr, std::move(cursor));
  } else {
    co_return LBAMapping::create_direct(std::move(cursor));
  }
}

base_iertr::future<LBAMapping> LBAMapping::refresh()
{
  if (is_viewable()) {
    return base_iertr::make_ready_future<LBAMapping>(*this);
  }
  return seastar::do_with(
    direct_cursor,
    indirect_cursor,
    [](auto &direct_cursor, auto &indirect_cursor) {
    return seastar::futurize_invoke([&direct_cursor] {
      if (direct_cursor) {
	return direct_cursor->refresh();
      }
      return base_iertr::now();
    }).si_then([&indirect_cursor] {
      if (indirect_cursor) {
	return indirect_cursor->refresh();
      }
      return base_iertr::now();
    }).si_then([&direct_cursor, &indirect_cursor] {
      return LBAMapping(direct_cursor, indirect_cursor);
    });
  });
}

base_iertr::future<> LBAMapping::co_refresh()
{
  if (is_viewable()) {
    co_return;
  }
  if (direct_cursor) {
    co_await direct_cursor->refresh();
  }
  if (indirect_cursor) {
    co_await indirect_cursor->refresh();
  }
}

bool LBAMapping::is_initial_pending() const {
  return direct_cursor->is_initial_pending();
}

} // namespace crimson::os::seastore
