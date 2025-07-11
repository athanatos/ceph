// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/btree/fixed_kv_node.h"
#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore {

#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *ptr)
{
  intrusive_ptr_add_ref(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
    logger().debug("intrusive_ptr_add_ref: {}", *ptr);
}

void intrusive_ptr_release(CachedExtent *ptr)
{
  logger().debug("intrusive_ptr_release: {}", *ptr);
  intrusive_ptr_release(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
}

#endif

std::ostream &operator<<(std::ostream &out, CachedExtent::extent_state_t state)
{
  switch (state) {
  case CachedExtent::extent_state_t::INITIAL_WRITE_PENDING:
    return out << "INITIAL_WRITE_PENDING";
  case CachedExtent::extent_state_t::MUTATION_PENDING:
    return out << "MUTATION_PENDING";
  case CachedExtent::extent_state_t::CLEAN:
    return out << "CLEAN";
  case CachedExtent::extent_state_t::DIRTY:
    return out << "DIRTY";
  case CachedExtent::extent_state_t::EXIST_CLEAN:
    return out << "EXIST_CLEAN";
  case CachedExtent::extent_state_t::EXIST_MUTATION_PENDING:
    return out << "EXIST_MUTATION_PENDING";
  case CachedExtent::extent_state_t::INVALID:
    return out << "INVALID";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext)
{
  return ext.print(out);
}

CachedExtent::~CachedExtent()
{
  if (parent_index) {
    assert(is_linked());
    parent_index->erase(*this);
  }
}
CachedExtent* CachedExtent::get_transactional_view(Transaction &t) {
  return get_transactional_view(t.get_trans_id());
}

CachedExtent* CachedExtent::get_transactional_view(transaction_id_t tid) {
  auto it = mutation_pending_extents.find(tid, trans_spec_view_t::cmp_t());
  if (it != mutation_pending_extents.end()) {
    return (CachedExtent*)&(*it);
  } else {
    return this;
  }
}

std::ostream &LogicalCachedExtent::print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr
      << ", seen=" << seen_by_users;
  return print_detail_l(out);
}

void CachedExtent::set_invalid(Transaction &t) {
  state = extent_state_t::INVALID;
  if (trans_view_hook.is_linked()) {
    trans_view_hook.unlink();
  }
  on_invalidated(t);
}

std::pair<bool, CachedExtent::viewable_state_t>
CachedExtent::is_viewable_by_trans(Transaction &t) {
  if (!is_valid()) {
    return std::make_pair(false, viewable_state_t::invalid);
  }

  auto trans_id = t.get_trans_id();
  if (is_pending()) {
    ceph_assert(is_pending_in_trans(trans_id));
    return std::make_pair(true, viewable_state_t::pending);
  }

  // shared by multiple transactions
  assert(t.is_in_read_set(this));
  assert(is_stable_ready());

  auto cmp = trans_spec_view_t::cmp_t();
  if (mutation_pending_extents.find(trans_id, cmp) !=
      mutation_pending_extents.end()) {
    return std::make_pair(false, viewable_state_t::stable_become_pending);
  }

  if (retired_transactions.find(trans_id, cmp) !=
      retired_transactions.end()) {
    assert(t.is_stable_extent_retired(get_paddr(), get_length()));
    return std::make_pair(false, viewable_state_t::stable_become_retired);
  }

  return std::make_pair(true, viewable_state_t::stable);
}

std::ostream &operator<<(
  std::ostream &out,
  CachedExtent::viewable_state_t state)
{
  switch(state) {
  case CachedExtent::viewable_state_t::stable:
    return out << "stable";
  case CachedExtent::viewable_state_t::pending:
    return out << "pending";
  case CachedExtent::viewable_state_t::invalid:
    return out << "invalid";
  case CachedExtent::viewable_state_t::stable_become_retired:
    return out << "stable_become_retired";
  case CachedExtent::viewable_state_t::stable_become_pending:
    return out << "stable_become_pending";
  default:
    __builtin_unreachable();
  }
}

bool BufferSpace::is_range_loaded(extent_len_t offset, extent_len_t length) const
{
  assert(length > 0);
  assert(offset + length <= extent_length);

  if (is_fully_loaded()) {
    return true;
  }

  auto &buffer_map = *std::get_if<map_t>(&buffer);

  auto i = buffer_map.upper_bound(offset);
  if (i == buffer_map.begin()) {
    return false;
  }
  --i;
  auto& [i_offset, i_bl] = *i;
  assert(offset >= i_offset);
  assert(i_bl.length() > 0);
  if (offset + length > i_offset + i_bl.length()) {
    return false;
  } else {
    return true;
  }
}

ceph::bufferlist BufferSpace::get_buffer(extent_len_t offset, extent_len_t length) const
{
  assert(length > 0);
  assert(offset + length <= extent_length);
  struct {
    extent_len_t offset;
    extent_len_t length;
    ceph::bufferlist operator()(const ceph::bufferptr &ptr) {
      ceph::bufferlist bl;
      bl.append(ceph::bufferptr(ptr, offset, length));
      return bl;
    }
    ceph::bufferlist operator()(const map_t &buffer_map) {
      assert(length > 0);
      auto i = buffer_map.upper_bound(offset);
      assert(i != buffer_map.begin());
      --i;
      auto& [i_offset, i_bl] = *i;
      assert(offset >= i_offset);
      assert(i_bl.length() > 0);
      assert(offset + length <= i_offset + i_bl.length());
      ceph::bufferlist res;
      res.substr_of(i_bl, offset - i_offset, length);
      return res;
    }
  } visitor{offset, length};
  return std::visit(visitor, buffer);
}

extent_len_t iter_to_start(
  const BufferSpace::map_t &buffer_map, auto iter)
{
  if (iter == buffer_map.end()) {
    // tolerate adding 1 to check adjacency
    return std::numeric_limits<decltype(iter->first)>::max() - 1;
  }
  return iter->first;
}

extent_len_t iter_to_end(
  const BufferSpace::map_t &buffer_map, auto iter)
{
  if (iter == buffer_map.end()) {
    // tolerate adding 1 to check adjacency
    return std::numeric_limits<decltype(iter->first)>::max() - 1;
  }
  return iter->first + iter->second.length();
}


std::pair<BufferSpace::map_t::iterator, BufferSpace::map_t::iterator>
get_adjacent_range(
  BufferSpace::map_t &buffer_map,
  extent_len_t offset, extent_len_t length)
{
  // Find first entry adjacent to [offset, offset + length)
  auto from_iter = buffer_map.lower_bound(offset);
  if (from_iter != buffer_map.begin()) {
    --from_iter;
    if (iter_to_end(buffer_map, from_iter) + 1 < offset) {
      ++from_iter;
    }
  }

  // Find one past last entry adjacent to [offset, offset + length)
  auto to_iter = buffer_map.upper_bound(offset + length);
  if (to_iter != buffer_map.end() &&
      ((offset + length + 1) >= iter_to_start(buffer_map, to_iter))) {
    ++to_iter;
  }
  return {from_iter, to_iter};
}

load_ranges_t BufferSpace::load_ranges(extent_len_t offset, extent_len_t length)
{
  assert(length > 0);
  assert(offset + length <= extent_length);

  load_ranges_t ret;
  if (is_fully_loaded()) {
    return ret;
  }

  if (loaded_length == 0 && length == extent_length) {
    auto ptr = bufferptr(length);
    buffer = ptr;
    ret.push_back(0, ptr);
    loaded_length = length;
    return ret;
  }

  auto &buffer_map = *std::get_if<map_t>(&buffer);
  const auto [from_iter, to_iter] = get_adjacent_range(
    buffer_map, offset, length);
  auto next_iter = to_iter;

  bufferlist bl;
  if (auto next_iter_offset = iter_to_start(buffer_map, next_iter);
      next_iter_offset > offset) {
    auto bp = create_extent_ptr_rand(
      std::min(next_iter_offset - offset, length)
    );
    bl.append(bp);
    ret.push_back(offset, bp);
  }
  while (next_iter != to_iter) {
    bl.append(next_iter->second);
    auto next_hole_offset = next_iter->first + next_iter->second.length();
    ++next_iter;
    if (auto next_iter_offset = iter_to_start(buffer_map, next_iter);
	next_hole_offset < next_iter_offset) {
      auto bp = create_extent_ptr_rand(
	std::min(next_iter_offset, offset + length) - next_iter_offset
      );
      bl.append(bp);
      ret.push_back(next_hole_offset, bp);
    }
  }
  buffer_map.erase(from_iter, to_iter);
  buffer_map.emplace(
    std::min(offset, iter_to_start(buffer_map, from_iter)),
    bl);
  loaded_length += ret.length;

  if (extent_length == loaded_length) {
    auto ptr = to_full_ptr();
    // adjust ret since the ptr has been rebuilt
    for (load_range_t &range : ret.ranges) {
      auto range_length = range.ptr.length();
      range.ptr = ceph::bufferptr(ptr, range.offset, range_length);
    }
  }
  
  return ret;
}

void BufferSpace::overwrite(extent_len_t offset, bufferlist in_bl)
{
  assert(offset + in_bl.length() <= extent_length);
  if (auto *bp = std::get_if<bufferptr>(&buffer)) {
    auto iter = in_bl.cbegin();
    iter.copy(in_bl.length(), bp->c_str() + offset);
    return;
  }

  auto &buffer_map = std::get<map_t>(buffer);
  const auto [from_iter, to_iter] = get_adjacent_range(
    buffer_map, offset, in_bl.length());
  bufferlist new_bl;
  const auto iter_start = iter_to_start(buffer_map, from_iter);
  if (iter_start < offset) {
    new_bl.substr_of(from_iter->second, 0, iter_start - offset);
  }
  new_bl.append(in_bl);

  if (from_iter != to_iter) {
    auto last_iter = to_iter;
    --last_iter;
    const auto iter_end = iter_to_end(buffer_map, last_iter);
    const auto overwrite_end = offset + in_bl.length();
    if (iter_end > overwrite_end) {
      bufferlist bl;
      const auto tail_offset = iter_end - overwrite_end;
      bl.substr_of(last_iter->second, tail_offset, iter_end - tail_offset);
      new_bl.append(bl);
    }
  }

  extent_len_t removing = 0;
  for (auto iter = from_iter; iter != to_iter; ++iter) {
    removing += iter->second.length();
  }
  assert(new_bl.length() > removing);
  loaded_length += new_bl.length() - removing;

  buffer_map.erase(from_iter, to_iter);
  buffer_map.emplace(
    std::min(offset, iter_start),
    new_bl);
  
  if (loaded_length == extent_length) {
    to_full_ptr();
  }
}

ceph::bufferptr BufferSpace::to_full_ptr()
{
  if (auto ptr = std::get_if<bufferptr>(&buffer)) {
    return *ptr;
  }

  auto &buffer_map = *std::get_if<map_t>(&buffer);

  assert(extent_length > 0);
  assert(buffer_map.size() == 1);
  auto it = buffer_map.begin();
  auto& [i_off, i_buf] = *it;
  assert(i_off == 0);
  if (!i_buf.is_contiguous()) {
    // Allocate page aligned ptr, also see create_extent_ptr_*()
    i_buf.rebuild();
  }
  assert(i_buf.get_num_buffers() == 1);
  ceph::bufferptr ptr(i_buf.front());
  assert(ptr.is_page_aligned());
  assert(ptr.length() == extent_length);
  buffer = ptr;
  return ptr;
}

}
