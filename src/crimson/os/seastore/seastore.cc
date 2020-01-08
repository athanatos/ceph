// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using crimson::common::local_conf;

namespace crimson::os::seastore {

struct SeastoreCollection final : public FuturizedCollection {
  template <typename... T>
  SeastoreCollection(T&&... args) : FuturizedCollection(std::forward<T>(args)...) {}
};

SeaStore::SeaStore(const std::string& path)
{}

SeaStore::~SeaStore() = default;

seastar::future<> SeaStore::mount()
{
  return seastar::now();
}

seastar::future<> SeaStore::umount()
{
  return seastar::now();
}

seastar::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  return seastar::now();
}

store_statfs_t SeaStore::stat() const
{
  logger().debug("{}", __func__);
  store_statfs_t st;
  return st;
}

seastar::future<std::vector<ghobject_t>, ghobject_t>
SeaStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  return seastar::make_ready_future<std::vector<ghobject_t>, ghobject_t>(
    std::vector<ghobject_t>(), end);
}

seastar::future<CollectionRef> SeaStore::create_new_collection(const coll_t& cid)
{
  auto c = _get_collection(cid);
  return seastar::make_ready_future<CollectionRef>(c);
}

seastar::future<CollectionRef> SeaStore::open_collection(const coll_t& cid)
{
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<std::vector<coll_t>> SeaStore::list_collections()
{
  return seastar::make_ready_future<std::vector<coll_t>>();
}

SeaStore::read_errorator::future<ceph::bufferlist> SeaStore::read(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  uint32_t op_flags)
{
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

SeaStore::get_attr_errorator::future<ceph::bufferptr> SeaStore::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name) const
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
                __func__, c->get_cid(), oid);
  return crimson::ct_error::enoent::make();
}

SeaStore::get_attrs_ertr::future<SeaStore::attrs_t> SeaStore::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
		 __func__, c->get_cid(), oid);
  return crimson::ct_error::enoent::make();
}

seastar::future<SeaStore::omap_values_t>
SeaStore::omap_get_values(CollectionRef ch,
                           const ghobject_t& oid,
                           const omap_keys_t& keys)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
                __func__, c->get_cid(), oid);
  return seastar::make_ready_future<omap_values_t>();
}

seastar::future<bool, SeaStore::omap_values_t>
SeaStore::omap_get_values(
    CollectionRef ch,
    const ghobject_t &oid,
    const std::optional<string> &start
  ) {
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug(
    "{} {} {}",
    __func__, c->get_cid(), oid);
  return seastar::make_ready_future<bool, omap_values_t>(
    false, omap_values_t());
}

seastar::future<> SeaStore::do_transaction(CollectionRef ch,
                                            ceph::os::Transaction&& t)
{
  using ceph::os::Transaction;
  int r = 0;
  try {
    auto i = t.begin();
    while (i.have_op()) {
      r = 0;
      switch (auto op = i.decode_op(); op->op) {
      case Transaction::OP_NOP:
	break;
      case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid(op->cid);
	ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
	if (r == -ENOENT) {
	  r = 0;
	}
      }
      break;
      case Transaction::OP_TOUCH:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _touch(cid, oid);
      }
      break;
      case Transaction::OP_WRITE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        ceph::bufferlist bl;
        i.decode_bl(bl);
        r = _write(cid, oid, off, len, bl, fadvise_flags);
      }
      break;
      case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        r = _truncate(cid, oid, off);
      }
      break;
      case Transaction::OP_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        std::string name = i.decode_string();
        ceph::bufferlist bl;
        i.decode_bl(bl);
        std::map<std::string, bufferptr> to_set;
        to_set[name] = bufferptr(bl.c_str(), bl.length());
        r = _setattrs(cid, oid, to_set);
      }
      break;
      case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        r = _create_collection(cid, op->split_bits);
      }
      break;
      case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        std::map<std::string, ceph::bufferlist> aset;
        i.decode_attrset(aset);
        r = _omap_set_values(cid, oid, std::move(aset));
      }
      break;
      case Transaction::OP_OMAP_SETHEADER:
      {
	const coll_t &cid = i.get_cid(op->cid);
	const ghobject_t &oid = i.get_oid(op->oid);
	ceph::bufferlist bl;
	i.decode_bl(bl);
	r = _omap_set_header(cid, oid, bl);
      }
      break;
      case Transaction::OP_OMAP_RMKEYS:
      {
	const coll_t &cid = i.get_cid(op->cid);
	const ghobject_t &oid = i.get_oid(op->oid);
	omap_keys_t keys;
	i.decode_keyset(keys);
	r = _omap_rmkeys(cid, oid, keys);
      }
      break;
      case Transaction::OP_OMAP_RMKEYRANGE:
      {
	const coll_t &cid = i.get_cid(op->cid);
	const ghobject_t &oid = i.get_oid(op->oid);
	string first, last;
	first = i.decode_string();
	last = i.decode_string();
	r = _omap_rmkeyrange(cid, oid, first, last);
      }
      break;
      case Transaction::OP_COLL_HINT:
      {
        ceph::bufferlist hint;
        i.decode_bl(hint);
	// ignored
	break;
      }
      default:
	logger().error("bad op {}", static_cast<unsigned>(op->op));
	abort();
      }
      if (r < 0) {
	break;
      }
    }
  } catch (std::exception &e) {
    logger().error("{} got exception {}", __func__, e);
    r = -EINVAL;
  }
  if (r < 0) {
    logger().error(" transaction dump:\n");
    JSONFormatter f(true);
    f.open_object_section("transaction");
    t.dump(&f);
    f.close_section();
    std::stringstream str;
    f.flush(str);
    logger().error("{}", str.str());
    ceph_assert(r == 0);
  }
  for (auto i : {
      t.get_on_applied(),
      t.get_on_commit(),
      t.get_on_applied_sync()}) {
    if (i) {
      i->complete(0);
    }
  }
  return seastar::now();
}

int SeaStore::_remove(const coll_t& cid, const ghobject_t& oid)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return 0;
}

int SeaStore::_touch(const coll_t& cid, const ghobject_t& oid)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return 0;
}

int SeaStore::_write(const coll_t& cid, const ghobject_t& oid,
                      uint64_t offset, size_t len, const ceph::bufferlist& bl,
                      uint32_t fadvise_flags)
{
  logger().debug("{} {} {} {} ~ {}",
                __func__, cid, oid, offset, len);
  assert(len == bl.length());
  return 0;
}

int SeaStore::_omap_set_values(
  const coll_t& cid,
  const ghobject_t& oid,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  logger().debug(
    "{} {} {} {} keys",
    __func__, cid, oid, aset.size());

  return 0;
}

int SeaStore::_omap_set_header(
  const coll_t& cid,
  const ghobject_t& oid,
  const ceph::bufferlist &header)
{
  logger().debug(
    "{} {} {} {} bytes",
    __func__, cid, oid, header.length());
  return 0;
}

int SeaStore::_omap_rmkeys(
  const coll_t& cid,
  const ghobject_t& oid,
  const omap_keys_t& aset)
{
  logger().debug(
    "{} {} {} {} keys",
    __func__, cid, oid, aset.size());
  return 0;
}

int SeaStore::_omap_rmkeyrange(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string &first,
  const std::string &last)
{
  logger().debug(
    "{} {} {} first={} last={}",
    __func__, cid, oid, first, last);
  return 0;
}

int SeaStore::_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  logger().debug("{} cid={} oid={} size={}",
                __func__, cid, oid, size);
  return 0;
}

int SeaStore::_setattrs(const coll_t& cid, const ghobject_t& oid,
                         std::map<std::string,bufferptr>& aset)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return 0;
}

int SeaStore::_create_collection(const coll_t& cid, int bits)
{
  return 0;
}

boost::intrusive_ptr<SeastoreCollection> SeaStore::_get_collection(const coll_t& cid)
{
  return new SeastoreCollection{cid};
}

seastar::future<> SeaStore::write_meta(const std::string& key,
					const std::string& value)
{
  return seastar::make_ready_future<>();
}

seastar::future<int, std::string> SeaStore::read_meta(const std::string& key)
{
  return seastar::make_ready_future<int, std::string>(0, "");
}

uuid_d SeaStore::get_fsid() const
{
  return osd_fsid;
}

}
