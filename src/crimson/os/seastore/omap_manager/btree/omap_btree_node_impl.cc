// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "seastar/core/thread.hh"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::omap_manager {
std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs)
{
  return out << "omap_inner_key (" << rhs.key_off<< " - " << rhs.key_len
             << " - " << rhs.laddr << ")";
}

std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs)
{
  return out << "omap_leaf_key_t (" << rhs.key_off<< " - " << rhs.key_len
             << " "<< rhs.val_off<<" - " << rhs.val_len << ")";
}

std::ostream &OMapInnerNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << get_meta().depth;
}

OMapInnerNode::get_value_ret
OMapInnerNode::get_value(omap_context_t oc, const std::string &key)
{
  logger().debug("{}: {} key = {}", "OMapInnerNode", __func__, key);
  auto child_pt = get_containing_child(key);
  auto laddr = child_pt->get_node_key().laddr;
  return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
    [oc, &key] (auto extent) {
    return extent->get_value(oc, key);
  });
}

OMapInnerNode::insert_ret
OMapInnerNode::insert(omap_context_t oc, std::string &key, std::string &value)
{
  logger().debug("{}: {}  {}->{}", "OMapInnerNode",  __func__, key, value);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_end());
  auto laddr = child_pt->get_node_key().laddr;
  return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
    [this, oc, child_pt, &key, &value] (auto extent) {
    auto size = extent->get_node_meta().depth > 1? key.size() + 1:
                                           key.size() + value.size() + 2;
    return extent->extent_is_overflow(size) ?
      split_entry(oc, key, child_pt, extent) :
      insert_ertr::make_ready_future<OMapNodeRef>(std::move(extent));
  }).safe_then([oc, &key, &value](OMapNodeRef extent) {
    return extent->insert(oc, key, value);
  });
}

OMapInnerNode::rm_key_ret
OMapInnerNode::rm_key(omap_context_t oc, const std::string &key)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  auto child_pt = get_containing_child(key);
  auto laddr = child_pt->get_node_key().laddr;
  return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
    [this, oc, &key, child_pt] (auto extent) {
    if (extent->extent_under_median() && get_node_size() > 1) {
      return merge_entry(oc, key, child_pt, extent);
    } else {
      return merge_entry_ertr::make_ready_future<OMapNodeRef>(std::move(extent));
    }
  }).safe_then([oc, &key](OMapNodeRef extent) mutable {
    return extent->rm_key(oc, key);
  });


}

OMapInnerNode::list_keys_ret
OMapInnerNode::list_keys(omap_context_t oc, std::vector<std::string> &result)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  return crimson::do_for_each(iter_begin(), iter_end(), [this, oc, &result](auto iter) {
    auto laddr = iter->get_node_key().laddr;
    return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
     [oc, &result] (auto &&extent) {
     return extent->list_keys(oc, result);
    });
  }).safe_then([] {
    return list_keys_ertr::make_ready_future<>();
  });
}

OMapInnerNode::list_ret
OMapInnerNode::list(omap_context_t oc,
                    std::vector<std::pair<std::string, std::string>> &result)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  return crimson::do_for_each(iter_begin(), iter_end(), [this, oc, &result](auto iter) {
    auto laddr = iter->get_node_key().laddr;
    return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
      [oc, &result] (auto &&extent) {
      return extent->list(oc, result);
    });
  }).safe_then([] {
    return list_ertr::make_ready_future<>();
  });
}

OMapInnerNode::clear_ret
OMapInnerNode::clear(omap_context_t oc)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  return crimson::do_for_each(iter_begin(), iter_end(), [this, oc] (auto iter) {
    auto laddr = iter->get_node_key().laddr;
    return omap_load_extent(oc, this, laddr, get_meta().depth - 1).safe_then(
      [oc] (auto &&extent) {
      return extent->clear(oc);
    }).safe_then([oc, laddr] {
      return oc.tm.dec_ref(oc.t, laddr);
    }).safe_then([] (auto ret){
      return clear_ertr::now();
    });
  });
}

OMapInnerNode::split_children_ret
OMapInnerNode:: make_split_children(omap_context_t oc, OMapNodeRef pnode)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  return omap_alloc_2extents<OMapInnerNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this] (auto &&ext_pair) {
      auto [left, right] = ext_pair;
      return split_children_ret(
             split_children_ertr::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

OMapInnerNode::full_merge_ret
OMapInnerNode::make_full_merge(omap_context_t oc, OMapNodeRef right, OMapNodeRef pnode)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  return omap_alloc_extent<OMapInnerNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapInnerNode>());
      return full_merge_ret(
        full_merge_ertr::ready_future_marker{},
        std::move(replacement));
  });
}

OMapInnerNode::make_balanced_ret
OMapInnerNode::make_balanced(omap_context_t oc, OMapNodeRef _right,
                             bool prefer_left, OMapNodeRef pnode)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  ceph_assert(_right->get_type() == type);
  return omap_alloc_2extents<OMapInnerNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this, _right, prefer_left] (auto &&replacement_pair){
      auto [replacement_left, replacement_right] = replacement_pair;
      auto &right = *_right->cast<OMapInnerNode>();
      return make_balanced_ret(
             make_balanced_ertr::ready_future_marker{},
             std::make_tuple(replacement_left, replacement_right,
             balance_into_new_nodes(*this, right, prefer_left,
                                    *replacement_left, *replacement_right)));
  });
}

OMapInnerNode::checking_parent_ret
OMapInnerNode::checking_parent(omap_context_t oc, std::string key,
               internal_iterator_t iter, OMapInnerNodeRef entry)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  if (extent_is_overflow(key.size() + 1)){
    if (get_node_meta().depth == oc.omap_root.depth) {
      assert (parent == nullptr);
      logger().debug("{}: {}----{}","OMapInnerNode",  __func__, "root");
      return omap_alloc_extent<OMapInnerNode>(oc, nullptr, OMAP_BLOCK_SIZE)
        .safe_then([this, oc, key, iter, entry](auto&& nroot) {
        omap_node_meta_t meta{entry->get_node_meta().depth + 1};
        nroot->set_meta(meta);
        entry->parent = nroot;
        omap_inner_key_t node_key;
        node_key.laddr = entry->get_laddr();
        node_key.key_off = 1;
        node_key.key_len = 1;
        nroot->journal_inner_insert(nroot->iter_begin(), node_key, "\0", nullptr);
        oc.omap_root.omap_root_laddr = nroot->get_laddr();
        oc.omap_root.depth = entry->get_node_meta().depth + 1;
        oc.omap_root.state = omap_root_state_t::MUTATED;
        return nroot->make_split_entry(oc, key, nroot->iter_begin(), entry)
          .safe_then([key, iter] (auto tuple) {
          auto left_node = std::get<0>(tuple);
          auto left = TCachedExtentRef<OMapInnerNode>(static_cast<OMapInnerNode*>(left_node.get()));
          auto right_node = std::get<1>(tuple);
          auto right = TCachedExtentRef<OMapInnerNode>(static_cast<OMapInnerNode*>(right_node.get()));
          auto pivot = std::get<2>(tuple);
          if (pivot >key)
            return checking_parent_ret(
                   checking_parent_ertr::ready_future_marker{},
                   std::make_pair(left, left->iter_idx(iter.get_index())));
          else
            return checking_parent_ret(
                   checking_parent_ertr::ready_future_marker{},
                   std::make_pair(right,
                    right->iter_idx(iter.get_index() - left->get_size())));
         });
      });
    } else {
      logger().debug("{}: {}----{}","OMapInnerNode",  __func__, "parent");
      auto parent_key = iter_begin().get_node_val();
      auto parent_entry = TCachedExtentRef<OMapInnerNode>(static_cast<OMapInnerNode*>(parent.get()));
      auto parent_ite = parent_entry->get_containing_child(parent_key);
      return parent_entry->make_split_entry(oc, key, parent_ite, entry)
        .safe_then([iter, key] (auto tuple){
          auto left_node = std::get<0>(tuple);
          auto left = TCachedExtentRef<OMapInnerNode>(static_cast<OMapInnerNode*>(left_node.get()));
          auto right_node = std::get<1>(tuple);
          auto right = TCachedExtentRef<OMapInnerNode>(static_cast<OMapInnerNode*>(right_node.get()));
          auto pivot = std::get<2>(tuple);
          if (pivot >key)
            return checking_parent_ret(
                   checking_parent_ertr::ready_future_marker{},
                   std::make_pair(left, left->iter_idx(iter.get_index())));
          else
            return checking_parent_ret(
                   checking_parent_ertr::ready_future_marker{},
                   std::make_pair(right,
                     right->iter_idx(iter.get_index() - left->get_size())));
      });
    }
  } else {
    logger().debug("{}: {}----{}","OMapInnerNode",  __func__, "not overflow");
    return checking_parent_ret(
      checking_parent_ertr::ready_future_marker{},
      std::make_pair(entry, iter));
  }
}


OMapInnerNode::make_split_entry_ret
OMapInnerNode::make_split_entry(omap_context_t oc, std::string key,
	                     internal_iterator_t iter, OMapNodeRef entry)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  if (!is_pending()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_index());
    mut->parent = this->parent;
    return mut->make_split_entry(oc, key, mut_iter, entry);
  }
  return entry->make_split_children(oc, this)
    .safe_then([this, oc, key, iter, entry] (auto tuple){
    auto [left, right, pivot] = tuple;
    logger().debug(
       "{}: *this {} entry {} into left {} right {}", __func__,
       *this, *entry, *left, *right);
    auto left_key = iter.get_node_key();
    left_key.laddr = left->get_laddr();
    journal_inner_update(iter, left_key, maybe_get_delta_buffer());
    return checking_parent(oc, pivot, iter+1, this)
      .safe_then([this, oc, left = left, right = right, pivot = pivot, entry] (auto pair) {
      auto [extent, iter] = pair;
      omap_inner_key_t right_key;
      right_key.laddr = right->get_laddr();
      right_key.key_len = pivot.size() + 1;
      right_key.key_off = iter.get_index() == 0 ?
                          right_key.key_len :
                          (iter - 1).get_node_key().key_off + pivot.size() + 1;
      extent->journal_inner_insert(iter, right_key, pivot,
                       extent->maybe_get_delta_buffer());
      //retire extent
      return oc.tm.dec_ref(oc.t, entry->get_laddr())
        .safe_then([left, right, pivot] (auto ret) {
        return make_split_entry_ret(
          make_split_entry_ertr::ready_future_marker{},
          std::make_tuple(left, right, pivot));
      });
    });
  });

}

OMapInnerNode::split_entry_ret
OMapInnerNode::split_entry(omap_context_t oc, std::string &key,
                      internal_iterator_t iter, OMapNodeRef entry)
{
  logger().debug("{}: {} key = {}","OMapInnerNode",  __func__, key);
  return make_split_entry(oc, key, iter, entry).safe_then([&key] (auto tuple) {
    auto [left, right, pivot] = tuple;
    return split_entry_ertr::make_ready_future<OMapNodeRef>(
       pivot > key ? left: right);
  });
}


OMapInnerNode::merge_entry_ret
OMapInnerNode::merge_entry(omap_context_t oc, const std::string &key,
                           internal_iterator_t iter, OMapNodeRef entry)
{
  logger().debug("{}: {}","OMapInnerNode",  __func__);
  if (!is_pending()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_index());
    mut->parent = this->parent;
    return mut->merge_entry(oc, key, mut_iter, entry);
  }
  logger().debug("{}: {}, {}", __func__, *this, *entry);
  auto is_left = (iter + 1) == iter_end();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return omap_load_extent(oc, this, donor_iter->get_node_key().laddr,  get_meta().depth - 1)
    .safe_then([this, oc, &key, iter, entry, donor_iter, is_left]
      (auto &&donor) mutable {
    auto [l, r] = is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->extent_under_median()) {
      logger().debug("{}::merge_entry make_full_merge l {} r {}", __func__, *l, *r);
      return l->make_full_merge(oc, r, this)
        .safe_then([this, oc, &key, iter, entry, donor_iter, is_left, l, r, liter, riter]
        (auto &&replacement){
        auto replace_key = liter.get_node_key();
        replace_key.laddr = replacement->get_laddr();
        journal_inner_update(liter, replace_key, maybe_get_delta_buffer());
        journal_inner_remove(riter, maybe_get_delta_buffer());
        //retire extent
        std::list<laddr_t> dec_laddrs;
        dec_laddrs.push_back(l->get_laddr());
        dec_laddrs.push_back(r->get_laddr());
        return omap_retire_node(oc, dec_laddrs)
          .safe_then([replacement] (auto &&ret) {
            return merge_entry_ertr::make_ready_future<OMapNodeRef>(replacement);
        });
      });
    } else {
      logger().debug("{}::merge_entry balanced l {} r {}", __func__, *l, *r);
      return l->make_balanced(oc, r, !is_left, this)
        .safe_then([this, oc, &key, iter, entry, donor_iter, is_left, l = l, r = r, liter, riter]
        (auto tuple) {
        auto [replacement_l, replacement_r, pivot] = tuple;
        auto left_key = liter.get_node_key();
        left_key.laddr = replacement_l->get_laddr();
        journal_inner_update(liter, left_key, maybe_get_delta_buffer());

        return checking_parent(oc, pivot, riter, this)
          .safe_then([oc, &key, pivot, l = l, r = r, replacement_l = replacement_l,
           replacement_r = replacement_r, entry] (auto pair) {
           auto [extent, riter] = pair;
           omap_inner_key_t right_key;
           right_key.laddr = replacement_r->get_laddr();
           right_key.key_len = pivot.size() + 1;
           right_key.key_off = riter.get_index() == 0?
                               right_key.key_len :
                               (riter - 1).get_node_key().key_off + right_key.key_len;
           extent->journal_inner_replace(riter, right_key, pivot, extent->maybe_get_delta_buffer());
          // retire extent
          std::list<laddr_t> dec_laddrs;
          dec_laddrs.push_back(l->get_laddr());
          dec_laddrs.push_back(r->get_laddr());
          return extent->omap_retire_node(oc, dec_laddrs)
            .safe_then([&key, pivot, replacement_l, replacement_r] (auto &&ret) {
            return merge_entry_ertr::make_ready_future<OMapNodeRef>(
                   key >= pivot ? replacement_r : replacement_l);
          });
        });
      });
    }
  });

}

OMapInnerNode::internal_iterator_t
OMapInnerNode::get_containing_child(const std::string &key)
{
  for (auto i = iter_begin(); i != iter_end(); ++i) {
    if (i.contains(key))
      return i;
  }
  ceph_assert( 0 == "invalid");
  return iter_end();
}

std::ostream &OMapLeafNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
         << ", depth=" << get_meta().depth;
}

OMapLeafNode::get_value_ret
OMapLeafNode::get_value(omap_context_t oc, const std::string &key)
{
  logger().debug("{}: {} key = {}","OMapLeafNode", __func__, key);
  auto ite = find_string_key(key);
  if (ite != iter_end()) {
    auto value = ite->get_string_val();
    return get_value_ret(
      get_value_ertr::ready_future_marker{},
      std::make_pair(key, value));
  } else {
    return get_value_ret(
      get_value_ertr::ready_future_marker{},
      std::make_pair(key, ""));
  }
}

OMapLeafNode::insert_ret
OMapLeafNode::insert(omap_context_t oc, std::string &key, std::string &value)
{
  logger().debug("{}: {}, {} -> {}","OMapLeafNode", __func__, key, value);
  ceph_assert(!extent_is_overflow(key.size() + value.size() + 2));
  if (!is_pending()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
    mut->parent = this->parent;
    return mut->insert(oc, key, value);
  }
  auto replace_pt = find_string_key(key);
  if (replace_pt != iter_end()) {
    journal_leaf_update(replace_pt, key, value, maybe_get_delta_buffer());
  } else {
    auto insert_pt = string_lower_bound(key);
    journal_leaf_insert(insert_pt, key, value, maybe_get_delta_buffer());

  logger().debug(
    "{}: {} inserted {}->{} {}"," OMapLeafNode",  __func__,
    insert_pt.get_node_key(),
    insert_pt.get_node_val(),
    insert_pt.get_string_val());
  }
  return insert_ret(
    insert_ertr::ready_future_marker{},
    std::make_pair(key, value));

}

OMapLeafNode::rm_key_ret
OMapLeafNode::rm_key(omap_context_t oc, const std::string &key)
{
  logger().debug("{}: {} : {}","OMapLeafNode",  __func__, key);
  if(!is_pending()) {
    auto mut =  oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
    mut->parent = this->parent;
    return mut->rm_key(oc, key);
  }

  auto rm_pt = find_string_key(key);
  if (rm_pt != iter_end()) {
    journal_leaf_remove(rm_pt, maybe_get_delta_buffer());
    logger().debug(
      "{}: removed {}->{} {}", __func__,
      rm_pt->get_node_key(),
      rm_pt->get_node_val(),
      rm_pt->get_string_val());
      return rm_key_ertr::make_ready_future<bool>(true);
  } else {
    return rm_key_ertr::make_ready_future<bool>(false);
  }

}

OMapLeafNode::list_keys_ret
OMapLeafNode::list_keys(omap_context_t oc, std::vector<std::string> &result)
{
  logger().debug("{}: {}","OMapLeafNode",  __func__);
  for (auto iter = iter_begin(); iter != iter_end(); iter++) {
    result.push_back(iter->get_node_val());
  }
  return list_keys_ertr::make_ready_future<>();

}

OMapLeafNode::list_ret
OMapLeafNode::list(omap_context_t oc,
                   std::vector<std::pair<std::string, std::string>> &result)
{
  logger().debug("{}: {}", "OMapLeafNode", __func__);
  for (auto iter = iter_begin(); iter != iter_end(); iter++) {
    result.push_back({iter->get_node_val(), iter->get_string_val()});
  }
  return list_ertr::make_ready_future<>();
}

OMapLeafNode::clear_ret
OMapLeafNode::clear(omap_context_t oc)
{
  return clear_ertr::now();
}

OMapLeafNode::split_children_ret
OMapLeafNode:: make_split_children(omap_context_t oc, OMapNodeRef pnode)
{
  logger().debug("{}: {}","OMapLeafNode",  __func__);
  return omap_alloc_2extents<OMapLeafNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this] (auto &&ext_pair) {
      auto [left, right] = ext_pair;
      return split_children_ret(
             split_children_ertr::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

OMapLeafNode::full_merge_ret
OMapLeafNode::make_full_merge(omap_context_t oc, OMapNodeRef right, OMapNodeRef pnode)
{
  ceph_assert(right->get_type() == type);
  logger().debug("{}: {}","OMapLeafNode",  __func__);
  return omap_alloc_extent<OMapLeafNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapLeafNode>());
      return full_merge_ret(
        full_merge_ertr::ready_future_marker{},
        std::move(replacement));
  });
}

OMapLeafNode::make_balanced_ret
OMapLeafNode::make_balanced(omap_context_t oc, OMapNodeRef _right, bool prefer_left, OMapNodeRef pnode)
{
  ceph_assert(_right->get_type() == type);
  logger().debug("{}: {}", "OMapLeafNode",  __func__);
  return omap_alloc_2extents<OMapLeafNode>(oc, pnode, OMAP_BLOCK_SIZE)
    .safe_then([this, _right, prefer_left] (auto &&replacement_pair) {
      auto [replacement_left, replacement_right] = replacement_pair;
      auto &right = *_right->cast<OMapLeafNode>();
      return make_balanced_ret(
             make_balanced_ertr::ready_future_marker{},
             std::make_tuple(
               replacement_left, replacement_right,
               balance_into_new_nodes(
                 *this, right, prefer_left,
                 *replacement_left, *replacement_right)));
  });
}


TransactionManager::read_extent_ertr::future<OMapNodeRef>
omap_load_extent(omap_context_t oc, OMapNodeRef pnode, laddr_t laddr, depth_t depth)
{
  ceph_assert(depth > 0);
  if (depth > 1) {
    return oc.tm.read_extents<OMapInnerNode>(oc.t, laddr, OMAP_BLOCK_SIZE).safe_then(
      [pnode](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->parent = pnode;
      return TransactionManager::read_extent_ertr::make_ready_future<OMapNodeRef>(std::move(e));
    });
  } else {
    return oc.tm.read_extents<OMapLeafNode>(oc.t, laddr, OMAP_BLOCK_SIZE).safe_then(
      [pnode](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->parent = pnode;
      return TransactionManager::read_extent_ertr::make_ready_future<OMapNodeRef>(std::move(e));
    });
  }
}

}
