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
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/cache.h"

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
  : segment_manager(segment_manager::create_ephemeral(
		      segment_manager::DEFAULT_TEST_EPHEMERAL)),
    onode_manager(onode_manager::create_ephemeral()),
    transaction_manager(new TransactionManager()),
    cache(std::make_unique<Cache>())
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

seastar::future<> SeaStore::do_transaction(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  return seastar::do_with(
    _t.begin(),
    transaction_manager->create_transaction(),
    std::move(_t),
    std::move(_ch),
    [this](auto &iter, auto &trans, auto &t, auto &ch) {
      return seastar::do_until(
	[this, &iter]() { return iter.have_op(); },
	[this, &iter, &trans, &t, &ch]() {
	  return _do_transaction_step(trans, ch, iter).safe_then(
	    [this, &trans] {
	      return transaction_manager->submit_transaction();
	    }).handle_error(
	      write_ertr::all_same_way([this, &t](auto e) {
		logger().error(" transaction dump:\n");
		JSONFormatter f(true);
		f.open_object_section("transaction");
		t.dump(&f);
		f.close_section();
		std::stringstream str;
		f.flush(str);
		logger().error("{}", str.str());
		abort();
	      }));
	}).then([this, &t]() {
	  for (auto i : {
	      t.get_on_applied(),
		t.get_on_commit(),
		t.get_on_applied_sync()}) {
	    if (i) {
	      i->complete(0);
	    }
	  }
	});
    });
}

SeaStore::write_ertr::future<> SeaStore::_do_transaction_step(
  TransactionRef &trans,
  CollectionRef &col,
  ceph::os::Transaction::iterator &i)
{
  using ceph::os::Transaction;
  try {
    switch (auto op = i.decode_op(); op->op) {
    case Transaction::OP_NOP:
      return write_ertr::now();
    case Transaction::OP_REMOVE:
    {
      coll_t cid = i.get_cid(op->cid);
      ghobject_t oid = i.get_oid(op->oid);
      return _remove(trans, cid, oid);
    }
    break;
    case Transaction::OP_TOUCH:
    {
      coll_t cid = i.get_cid(op->cid);
      ghobject_t oid = i.get_oid(op->oid);
      return _touch(trans, cid, oid);
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
      return _write(trans, cid, oid, off, len, bl, fadvise_flags);
    }
    break;
    case Transaction::OP_TRUNCATE:
    {
      coll_t cid = i.get_cid(op->cid);
      ghobject_t oid = i.get_oid(op->oid);
      uint64_t off = op->off;
      return _truncate(trans, cid, oid, off);
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
      return _setattrs(trans, cid, oid, to_set);
    }
    break;
    case Transaction::OP_MKCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _create_collection(trans, cid, op->split_bits);
    }
    break;
    case Transaction::OP_OMAP_SETKEYS:
    {
      coll_t cid = i.get_cid(op->cid);
      ghobject_t oid = i.get_oid(op->oid);
      std::map<std::string, ceph::bufferlist> aset;
      i.decode_attrset(aset);
      return _omap_set_values(trans, cid, oid, std::move(aset));
    }
    break;
    case Transaction::OP_OMAP_SETHEADER:
    {
      const coll_t &cid = i.get_cid(op->cid);
      const ghobject_t &oid = i.get_oid(op->oid);
      ceph::bufferlist bl;
      i.decode_bl(bl);
      return _omap_set_header(trans, cid, oid, bl);
    }
    break;
    case Transaction::OP_OMAP_RMKEYS:
    {
      const coll_t &cid = i.get_cid(op->cid);
      const ghobject_t &oid = i.get_oid(op->oid);
      omap_keys_t keys;
      i.decode_keyset(keys);
      return _omap_rmkeys(trans, cid, oid, keys);
    }
    break;
    case Transaction::OP_OMAP_RMKEYRANGE:
    {
      const coll_t &cid = i.get_cid(op->cid);
      const ghobject_t &oid = i.get_oid(op->oid);
      string first, last;
      first = i.decode_string();
      last = i.decode_string();
      return _omap_rmkeyrange(trans, cid, oid, first, last);
    }
    break;
    case Transaction::OP_COLL_HINT:
    {
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return write_ertr::now();
    }
    default:
      logger().error("bad op {}", static_cast<unsigned>(op->op));
      return crimson::ct_error::input_output_error::make();
    }
  } catch (std::exception &e) {
    logger().error("{} got exception {}", __func__, e);
    return crimson::ct_error::input_output_error::make();
  }
}

SeaStore::write_ertr::future<> SeaStore::_remove(
  TransactionRef &trans,
  const coll_t& cid, const ghobject_t& oid)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_touch(
  TransactionRef &trans,
  const coll_t& cid, const ghobject_t& oid)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_write(
  TransactionRef &trans,
  const coll_t& cid, const ghobject_t& oid,
  uint64_t offset, size_t len, const ceph::bufferlist& bl,
  uint32_t fadvise_flags)
{
  logger().debug("{} {} {} {} ~ {}",
                __func__, cid, oid, offset, len);
  assert(len == bl.length());

  return onode_manager->get_or_create_onode(cid, oid).safe_then([=, &bl](auto ref) {
    return;
  }).handle_error(
    crimson::ct_error::enoent::handle([]() {
      return;
    }),
    OnodeManager::open_ertr::pass_further{}
  );
}

SeaStore::write_ertr::future<> SeaStore::_omap_set_values(
  TransactionRef &trans,
  const coll_t& cid,
  const ghobject_t& oid,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  logger().debug(
    "{} {} {} {} keys",
    __func__, cid, oid, aset.size());

  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_set_header(
  TransactionRef &trans,
  const coll_t& cid,
  const ghobject_t& oid,
  const ceph::bufferlist &header)
{
  logger().debug(
    "{} {} {} {} bytes",
    __func__, cid, oid, header.length());
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_rmkeys(
  TransactionRef &trans,
  const coll_t& cid,
  const ghobject_t& oid,
  const omap_keys_t& aset)
{
  logger().debug(
    "{} {} {} {} keys",
    __func__, cid, oid, aset.size());
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_rmkeyrange(
  TransactionRef &trans,
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string &first,
  const std::string &last)
{
  logger().debug(
    "{} {} {} first={} last={}",
    __func__, cid, oid, first, last);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_truncate(
  TransactionRef &trans,
  const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  logger().debug("{} cid={} oid={} size={}",
                __func__, cid, oid, size);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_setattrs(
  TransactionRef &trans,
  const coll_t& cid, const ghobject_t& oid,
  std::map<std::string,bufferptr>& aset)
{
  logger().debug("{} cid={} oid={}",
                __func__, cid, oid);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_create_collection(
  TransactionRef &trans,
  const coll_t& cid, int bits)
{
  return write_ertr::now();
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
