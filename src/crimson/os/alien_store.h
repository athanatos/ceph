// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <seastar/core/future.hh>
#include "common/ceph_context.h"
#include "os/ObjectStore.h"
#include "osd/osd_types.h"

#include "crimson/thread/ThreadPool.h"

#include "futurized_collection.h"
#include "futurized_store.h"

namespace ceph::os {
class Transaction;

class AlienStore final : public FuturizedStore {
  constexpr static unsigned MAX_KEYS_PER_OMAP_GET_CALL = 32;

  const std::string path;
  uint64_t used_bytes = 0;
  uuid_d osd_fsid;
  unsigned int cpuid = 10;
  ObjectStore* store;
  std::unique_ptr<CephContext> cct;
public:
  mutable ceph::thread::ThreadPool tp{3, 128, cpuid};
  AlienStore(const std::string& path, ConfigValues* values);
  ~AlienStore() final;

  seastar::future<> stop() final;
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<ceph::bufferlist> read(CollectionRef c,
                                   const ghobject_t& oid,
                                   uint64_t offset,
                                   size_t len,
                                   uint32_t op_flags = 0) final;

  seastar::future<ceph::bufferptr> get_attr(CollectionRef c,
                                            const ghobject_t& oid,
                                            std::string_view name) const final;
  seastar::future<attrs_t> get_attrs(CollectionRef c,
                                     const ghobject_t& oid) final;

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

  seastar::future<> do_transaction(CollectionRef c,
                                   Transaction&& txn) final;

  seastar::future<> write_meta(const std::string& key,
                  const std::string& value) final;
  seastar::future<int, std::string> read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;
  seastar::future<store_statfs_t> stat() const final;
  unsigned get_max_attr_name_length() const final;
};
}
