// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"
#include "include/uuid.h"

#include "os/Transaction.h"
#include "crimson/os/futurized_store.h"

namespace crimson::os::seastore {

class SeastoreCollection;
class SegmentManager;
class OnodeManager;
class Cache;

class SeaStore final : public FuturizedStore {
  uuid_d osd_fsid;

public:

  SeaStore(const std::string& path);
  ~SeaStore() final;

  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  store_statfs_t stat() const final;

  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final;
  get_attr_errorator::future<ceph::bufferptr> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid);

  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  seastar::future<std::vector<ghobject_t>, ghobject_t> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  /// Retrieves paged set of values > start (if present)
  seastar::future<bool, omap_values_t> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(
    CollectionRef ch,
    ceph::os::Transaction&& txn) final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;
  seastar::future<int, std::string> read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;

  unsigned get_max_attr_name_length() const final {
    return 256;
  }

private:
  std::unique_ptr<SegmentManager> segment_manager;
  std::unique_ptr<OnodeManager> onode_manager;
  std::unique_ptr<Cache> cache;


  int _do_transaction_step(
    CollectionRef col,
    ceph::os::Transaction::iterator &i);

  int _remove(const coll_t& cid, const ghobject_t& oid);
  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, const ceph::bufferlist& bl,
	     uint32_t fadvise_flags);
  int _omap_set_values(
    const coll_t& cid,
    const ghobject_t& oid,
    std::map<std::string, ceph::bufferlist> &&aset);
  int _omap_set_header(
    const coll_t& cid,
    const ghobject_t& oid,
    const ceph::bufferlist &header);
  int _omap_rmkeys(
    const coll_t& cid,
    const ghobject_t& oid,
    const omap_keys_t& aset);
  int _omap_rmkeyrange(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string &first,
    const std::string &last);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _setattrs(const coll_t& cid, const ghobject_t& oid,
                std::map<std::string,bufferptr>& aset);
  int _create_collection(const coll_t& cid, int bits);

  boost::intrusive_ptr<SeastoreCollection> _get_collection(const coll_t& cid);
};

}
