// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <map>
#include <memory>
#include <boost/optional.hpp>

#include "common/hobject.h"
#include "osd/osd_types.h"
#include "common/interval_map.h"
#include "common/inline_variant.h"

/**
 * This class represents transactions which can be submitted to
 * a PGBackend.  For expediency, there are some constraints on
 * the operations submitted:
 * 1) Rename sources may only be referenced prior to the rename
 *    operation to the destination.
 * 2) The graph formed by edges of source->destination for clones
 *    (Create) and Renames must be acyclic.
 * 3) clone_range sources must not be modified by the same
 *    transaction
 */
class PGTransaction {
public:
  map<hobject_t, ObjectContextRef, hobject_t::BitwiseComparator> obc_map;

  class ObjectOperation {
  public:
    struct Init
    {
      struct None {};
      struct Create {};
      struct Clone {
	hobject_t source;
      };
      struct Rename {
	hobject_t source; // must be temp object
      };
    };
    using InitType = boost::variant<
      Init::None,
      Init::Create,
      Init::Clone,
      Init::Rename>;

    InitType init_type = Init::None();
    bool delete_first = false;

    bool is_delete() const {
      return boost::get<Init::None>(&init_type) != nullptr && delete_first;
    }
    bool is_none() const {
      return boost::get<Init::None>(&init_type) != nullptr && !delete_first;
    }
    bool is_fresh_object() const {
      return boost::get<Init::None>(&init_type) == nullptr;
    }
    bool has_source(hobject_t *source = nullptr) const {
      return match(
	init_type,
	[&](const Init::Clone &op) -> bool {
	  if (source)
	    *source = op.source;
	  return true;
	},
	[&](const Init::Rename &op) -> bool {
	  if (source)
	    *source = op.source;
	  return true;
	},
	[&](const Init::None &) -> bool { return false; },
	[&](const Init::Create &) -> bool { return false; });
    }

    bool clear_omap = false;
    boost::optional<uint64_t> truncate = boost::none;

    std::map<string, boost::optional<bufferlist> > attr_updates;

    enum class OmapUpdateType {Remove, Insert};
    std::vector<std::pair<OmapUpdateType, bufferlist> > omap_updates;

    boost::optional<bufferlist> omap_header;

    boost::optional<set<snapid_t> > updated_snaps;

    struct alloc_hint_t {
      uint64_t expected_object_size;
      uint64_t expected_write_size;
      uint32_t flags;
    };
    boost::optional<alloc_hint_t> alloc_hint;

    struct BufferUpdate {
      struct Write {
	bufferlist buffer;
	uint32_t fadvise_flags;
      };
      struct Zero {};
      struct CloneRange {
	hobject_t from;
	uint64_t offset;
	uint64_t len;
      };
    };
    using BufferUpdateType = boost::variant<
      BufferUpdate::Write,
      BufferUpdate::Zero,
      BufferUpdate::CloneRange>;

  private:
    struct Splitter {
      BufferUpdateType operator()(
	uint64_t offset,
	uint64_t len,
	BufferUpdateType &bu) {
	struct visitor : boost::static_visitor<BufferUpdateType> {
	  uint64_t offset;
	  uint64_t len;
	  visitor(uint64_t offset, uint64_t len) : offset(offset), len(len) {}
	  BufferUpdateType operator()(BufferUpdate::Write &w) const {
	    bufferlist bl;
	    bl.substr_of(w.buffer, offset, len);
	    return BufferUpdate::Write{bl, w.fadvise_flags};
	  }
	  BufferUpdateType operator()(BufferUpdate::Zero &) const {
	    return BufferUpdate::Zero();
	  }
	  BufferUpdateType operator()(BufferUpdate::CloneRange &c) const {
	    return BufferUpdate::CloneRange{c.from, c.offset + offset, len};
	  }
	};
	return boost::apply_visitor(visitor{offset, len}, bu);
      }
    };
  public:
    interval_map<uint64_t, BufferUpdateType, Splitter> buffer_updates;

    friend class PGTransaction;
  };
  map<hobject_t, ObjectOperation, hobject_t::BitwiseComparator> op_map;
private:
  ObjectOperation &get_object_op_for_modify(const hobject_t &hoid) {
    auto &op = op_map[hoid];
    assert(!op.is_delete());
    return op;
  }
public:
  void add_obc(
    ObjectContextRef obc) {
    assert(obc);
    obc_map[obc->obs.oi.soid] = obc;
  }
  /// Sets up state for new object
  void create(
    const hobject_t &hoid
    ) {
    auto &op = op_map[hoid];
    assert(op.is_none() || op.is_delete());
    op.init_type = ObjectOperation::Init::Create();
  }

  /// Sets up state for target cloned from source
  void clone(
    const hobject_t &target,       ///< [in] obj to clone to
    const hobject_t &source        ///< [in] obj to clone from
    ) {
    auto &op = op_map[target];
    assert(op.is_none() || op.is_delete());
    op.init_type = ObjectOperation::Init::Clone{source};
  }

  /// Sets up state for target renamed from source
  void rename(
    const hobject_t &target,       ///< [in] source (must be a temp object)
    const hobject_t &source        ///< [in] to, must not exist, be non-temp
    ) {
    assert(source.is_temp());
    assert(!target.is_temp());
    auto &op = op_map[target];
    assert(op.is_none() || op.is_delete());

    auto iter = op_map.find(source);
    if (iter != op_map.end()) {
      op = iter->second;
      op_map.erase(iter);
    }

    op.init_type = ObjectOperation::Init::Rename{source};
  }

  /// Remove
  void remove(
    const hobject_t &hoid          ///< [in] obj to remove
    ) {
    auto &op = get_object_op_for_modify(hoid);
    assert(!op.updated_snaps);
    op = ObjectOperation();
    op.delete_first = true;
  }

  void update_snaps(
    const hobject_t &hoid,         ///< [in] object for snaps
    set<snapid_t> &&snaps          ///< [in] snaps to update
    ) {
    auto &op = get_object_op_for_modify(hoid);
    assert(!op.updated_snaps);
    op.updated_snaps = std::move(snaps);
  }

  /// Clears, truncates
  void omap_clear(
    const hobject_t &hoid          ///< [in] object to clear omap
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.clear_omap = true;
    op.omap_updates.clear();
    op.omap_header = boost::none;
  }
  void truncate(
    const hobject_t &hoid,         ///< [in] object
    uint64_t off                   ///< [in] offset to truncate to
    ) {
    auto &op = get_object_op_for_modify(hoid);
    if (op.truncate && (*(op.truncate) >= off))
      return;
    op.buffer_updates.erase(off, std::numeric_limits<uint64_t>::max() - off);
    if (!op.is_fresh_object())
      op.truncate = off;
  }

  /// Attr ops
  void setattrs(
    const hobject_t &hoid,         ///< [in] object to write
    map<string, bufferlist> &attrs ///< [in] attrs, may be cleared
    ) {
    auto &op = get_object_op_for_modify(hoid);
    for (auto &&i: attrs) {
      op.attr_updates[i.first] = i.second;
    }
  }
  void setattr(
    const hobject_t &hoid,         ///< [in] object to write
    const string &attrname,        ///< [in] attr to write
    bufferlist &bl                 ///< [in] val to write, may be claimed
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.attr_updates[attrname] = bl;
  }
  void rmattr(
    const hobject_t &hoid,         ///< [in] object to write
    const string &attrname         ///< [in] attr to remove
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.attr_updates[attrname] = boost::none;
  }

  /// set alloc hint
  void set_alloc_hint(
    const hobject_t &hoid,         ///< [in] object (must exist)
    uint64_t expected_object_size, ///< [in]
    uint64_t expected_write_size,
    uint32_t flags
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.alloc_hint = ObjectOperation::alloc_hint_t{
      expected_object_size, expected_write_size, flags};
  }

  /// Buffer updates
  void write(
    const hobject_t &hoid,         ///< [in] object to write
    uint64_t off,                  ///< [in] off at which to write
    uint64_t len,                  ///< [in] len to write from bl
    bufferlist &bl,                ///< [in] bl to write will be claimed to len
    uint32_t fadvise_flags = 0     ///< [in] fadvise hint
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.buffer_updates.insert(
      off,
      len,
      ObjectOperation::BufferUpdate::Write{bl, fadvise_flags});
  }
  void clone_range(
    const hobject_t &from,         ///< [in] from
    const hobject_t &to,           ///< [in] to
    uint64_t fromoff,              ///< [in] offset
    uint64_t len,                  ///< [in] len
    uint64_t tooff                 ///< [in] offset
    ) {
    auto &op = get_object_op_for_modify(to);
    op.buffer_updates.insert(
      tooff,
      len,
      ObjectOperation::BufferUpdate::CloneRange{from, fromoff, len});
  }
  void zero(
    const hobject_t &hoid,         ///< [in] object
    uint64_t off,                  ///< [in] offset to start zeroing at
    uint64_t len                   ///< [in] amount to zero
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.buffer_updates.insert(
      off,
      len,
      ObjectOperation::BufferUpdate::Zero());
  }

  /// Omap updates
  void omap_setkeys(
    const hobject_t &hoid,         ///< [in] object to write
    bufferlist &keys_bl            ///< [in] encoded map<string, bufferlist>
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_updates.emplace_back(
      make_pair(
	ObjectOperation::OmapUpdateType::Insert,
	keys_bl));
  }
  void omap_setkeys(
    const hobject_t &hoid,         ///< [in] object to write
    map<string, bufferlist> &keys  ///< [in] omap keys, may be cleared
    ) {
    bufferlist bl;
    ::encode(keys, bl);
    omap_setkeys(hoid, bl);
  }
  void omap_rmkeys(
    const hobject_t &hoid,         ///< [in] object to write
    bufferlist &keys_bl            ///< [in] encode set<string>
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_updates.emplace_back(
      make_pair(
	ObjectOperation::OmapUpdateType::Remove,
	keys_bl));
  }
  void omap_rmkeys(
    const hobject_t &hoid,         ///< [in] object to write
    set<string> &keys              ///< [in] omap keys, may be cleared
    ) {
    bufferlist bl;
    ::encode(keys, bl);
    omap_rmkeys(hoid, bl);
  }
  void omap_setheader(
    const hobject_t &hoid,         ///< [in] object to write
    bufferlist &header             ///< [in] header
    ) {
    auto &op = get_object_op_for_modify(hoid);
    op.omap_header = header;
  }

  bool empty() const {
    return op_map.empty();
  }

  uint64_t get_bytes_written() const {
    uint64_t ret = 0;
    for (auto &&i: op_map) {
      for (auto &&j: i.second.buffer_updates) {
	ret += j.get_len();
      }
    }
    return ret;
  }

  void nop(
    const hobject_t &hoid ///< [in] obj to which we are doing nothing
    ) {
    get_object_op_for_modify(hoid);
  }

  /* Calls t() on all pair<hobject_t, ObjectOperation> & such that clone/rename
   * sinks are always called before clone sources
   *
   * TODO: add a fast path for the single object case and possibly the single
   * object clone from source case (make_writeable made a clone).
   *
   * This structure only requires that the source->sink graph be acyclic.
   * This is much more general than is actually required by ReplicatedPG.
   * Only 4 flavors of multi-object transactions actually happen:
   * 1) rename temp -> object for copyfrom
   * 2) clone head -> clone, modify head for make_writeable on normal head write
   * 3) clone clone -> head for rollback
   * 4) 2 + 3
   *
   * We can bypass the below logic for single object transactions trivially
   * (including case 1 above since temp doesn't show up again).
   * For 2-3, we could add something ad-hoc to ensure that they happen in the
   * right order, but it actually seems easier to just do the graph construction.
   */
  template <typename T>
  void safe_create_traverse(T &&t) {
    map<hobject_t, list<hobject_t>, hobject_t::BitwiseComparator> dgraph;
    list<hobject_t> stack;

    // Populate stack with roots, dgraph with edges
    for (auto &&opair: op_map) {
      hobject_t source;
      if (opair.second.has_source(&source)) {
	auto &l = dgraph[source];
	if (l.empty() && !op_map.count(source)) {
	  /* Source oids not in op_map need to be added as roots
	   * (but only once!) */
	  stack.push_back(source);
	}
	l.push_back(opair.first);
      } else {
	stack.push_back(opair.first);
      }
    }

    /* Why don't we need to worry about accessing the same node
     * twice?  dgraph nodes always have in-degree at most 1 because
     * the inverse graph nodes (source->dest) can have out-degree
     * at most 1 (only one possible source).  We do a post-order
     * depth-first traversal here to ensure we call f on children
     * before parents.
     */
    while (!stack.empty()) {
      hobject_t &cur = stack.front();
      auto diter = dgraph.find(cur);
      if (diter == dgraph.end()) {
	/* Leaf: pop and call t() */
	auto opiter = op_map.find(cur);
	if (opiter != op_map.end())
	  t(*opiter);
	stack.pop_front();
      } else {
	/* Internal node: push children onto stack, remove edge,
	 * recurse.  When this node is encountered again, it'll
	 * be a leaf */
	assert(!diter->second.empty());
	stack.splice(stack.begin(), diter->second);
	dgraph.erase(diter);
      }
    }
  }
};
using PGTransactionUPtr = std::unique_ptr<PGTransaction>;

#endif
