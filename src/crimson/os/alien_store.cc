#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "alien_collection.h"
#include "alien_store.h"
#include "futurized_store.h"

#include <boost/algorithm/string/trim.hpp>
#include <seastar/core/alien.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/Context.h"
#include "os/bluestore/BlueStore.h"
#include "os/ObjectStore.h"
#include "os/Transaction.h"

#include "crimson/common/log.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_filestore);
  }
}

class C_ALIEN_OnCommit : public Context {
  int cpuid;
  Context* on_commit = nullptr;
public:
  seastar::promise<int> alien_done;
  C_ALIEN_OnCommit(int id, ceph::os::Transaction& txn): cpuid(id) {
    if (txn.has_contexts()){
      on_commit = txn.get_on_commit();
    }
  }

  void finish(int) override {
    auto fut = seastar::alien::submit_to(cpuid,[this]{
      if (on_commit){
        on_commit->complete(0);
      }
      alien_done.set_value(1);
      return seastar::make_ready_future<>();
    });
    fut.wait();
  }
};

namespace ceph::os {

AlienStore::AlienStore(const std::string& path, ConfigValues* values)
  : path{path}

{
  tp.start();
  cct = std::unique_ptr<CephContext>(new CephContext(CEPH_ENTITY_TYPE_OSD));
  g_ceph_context = cct.get();
  cct->_conf.set_config_values(values);
  store = new BlueStore(cct.get(), path);

}
/*
seastar::future<> AlienStore::start()
{
  return tp.start();
}
*/
seastar::future<> AlienStore::stop()
{
  return tp.submit([this](){
    delete store;
    return 0;
  }).then([this](int r) {
    return tp.stop();
  });
}
AlienStore::~AlienStore()
{
}


seastar::future<> AlienStore::mount()
{
  logger().debug("{}", __func__);
  return tp.submit([this](){
    int r = store->mount();
    return r;
  }).then([] (int r){
    return seastar::now();
  });
}

seastar::future<> AlienStore::umount()
{
  logger().debug("{}", __func__);
  return tp.submit([this](){
      int r = store->umount();
      return r;
  }).then([] (int r){
    return seastar::now();
  });
}

seastar::future<> AlienStore::mkfs(uuid_d new_osd_fsid)
{
  logger().debug("{}", __func__);
  osd_fsid = new_osd_fsid;
  return tp.submit([this](){
      int r = store->mkfs();
      return r;
  }).then([] (int r){
    return seastar::now();
  });
}

seastar::future<std::vector<ghobject_t>, ghobject_t>
AlienStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  logger().debug("{}", __func__);
  std::vector<ghobject_t> objects;
  objects.reserve(limit);
  ghobject_t next;
  return seastar::do_with(std::move(objects),std::move(next),[=](auto &objects, auto &next) {
    return tp.submit([=, &objects, &next](){
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->collection_list(c->sch, start, end, store->get_ideal_list_max(), &objects, &next);
    }).then([&objects, &next](int r){
      return seastar::make_ready_future<std::vector<ghobject_t>, ghobject_t>(
                                         std::move(objects), std::move(next));
    });
  });
}

seastar::future<CollectionRef> AlienStore::create_new_collection(const coll_t& cid)
{
  logger().debug("{}", __func__);
  return tp.submit([this, cid](){
    return store->create_new_collection(cid);
  }).then([] (ObjectStore::CollectionHandle c){
    boost::intrusive_ptr<FuturizedCollection> ch (new AlienCollection(c));
    return seastar::make_ready_future<CollectionRef>(std::move(ch));
  });

}

seastar::future<CollectionRef> AlienStore::open_collection(const coll_t& cid)
{
  logger().debug("{}", __func__);
  return tp.submit([this, cid](){
    return store->open_collection(cid);
  }).then([this] (ObjectStore::CollectionHandle c){
    boost::intrusive_ptr<FuturizedCollection> ch (new AlienCollection(c));
    return seastar::make_ready_future<CollectionRef>(ch);
  });

}

seastar::future<std::vector<coll_t>> AlienStore::list_collections()
{
  logger().debug("{}", __func__);

  std::vector<coll_t> ls;
  return seastar::do_with(std::move(ls), [=](auto &ls){
    return tp.submit([this, &ls](){
      return store->list_collections(ls);
    }).then([&ls] (int r){
      return seastar::make_ready_future<std::vector<coll_t>>(std::move(ls));
    });
  });

}

seastar::future<ceph::bufferlist> AlienStore::read(CollectionRef ch,
                                            const ghobject_t& oid,
                                            uint64_t offset,
                                            size_t len,
                                            uint32_t op_flags)
{
  logger().debug("{}", __func__);
  return seastar::do_with(ceph::bufferlist{}, [=](auto &bl){
    return tp.submit([=, &bl](){
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->read(c->sch, oid, offset, len, bl, op_flags);
    }).then([&bl] (int r){
      return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
    });
  });

}

seastar::future<ceph::bufferptr> AlienStore::get_attr(CollectionRef ch,
                                                      const ghobject_t& oid,
                                                      std::string_view name) const
{
  logger().debug("{}", __func__);
  ceph::bufferptr value;
  return seastar::do_with(std::move(value),[=](auto &value) {
    return tp.submit([=, &value]() {
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->getattr(c->sch, oid, std::string(name).c_str(), value);
    }).then([oid, name, &value] (int r) {
      if (r == -ENOENT) {
        return seastar::make_exception_future<ceph::bufferptr>(
          EnoentException(fmt::format("object does not exist: {}", oid)));
      } else if (r == -ENODATA) {
        return seastar::make_exception_future<ceph::bufferptr>(
          EnoentException(fmt::format("attr does not exist: {}/{}", oid, name)));
      } else {
        return seastar::make_ready_future<ceph::bufferptr>(std::move(value));
      }
    });
  });
}

seastar::future<AlienStore::attrs_t> AlienStore::get_attrs(CollectionRef ch,
                                                     const ghobject_t& oid)
{
  logger().debug("{}", __func__);
  //std::map<std::string,ceph::bufferptr> aset;
  attrs_t aset;
  return seastar::do_with(std::move(aset),[=](auto &aset){
    return tp.submit([=, &aset](){
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->getattrs(c->sch, oid, aset);
    }).then([&aset] (int r) {
      return seastar::make_ready_future<attrs_t>(std::move(aset));
    });
  });

}

seastar::future<AlienStore::omap_values_t>
AlienStore::omap_get_values(CollectionRef ch,
                       const ghobject_t& oid,
                       const set<string>& keys)
{
  logger().debug("{}", __func__);
  //std::map<std::string, ceph::bufferlist> values;
  omap_values_t values;
  return seastar::do_with(std::move(values),[=](auto &values){
    return tp.submit([=, &values](){
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->omap_get_values(c->sch, oid, keys, &values);
    }).then([&values] (int r) {
      return seastar::make_ready_future<omap_values_t>(std::move(values));
    });
  });
}

seastar::future<bool, AlienStore::omap_values_t>
AlienStore::omap_get_values(CollectionRef c,
                            const ghobject_t &oid,
                            const std::optional<string> &start)
{
  logger().info("{}", __func__);
}

seastar::future<> AlienStore::do_transaction(CollectionRef ch,
                                             Transaction&& txn)
{
  logger().debug("{}", __func__);
  std::unique_ptr<Context> callback = std::unique_ptr<Context>(new C_ALIEN_OnCommit(seastar::engine().cpu_id(), txn));
  return seastar::do_with(std::move(txn), std::move(callback), [=](Transaction& txn, std::unique_ptr<Context> &callback){
    return tp.submit([=, &txn, &callback]() {
      txn.register_on_commit(callback.get());
      auto c =static_cast<AlienCollection*>(ch.get());
      int r = store->queue_transaction(c->sch, std::move(txn));
      return r;
    }).then([&callback] (int r) {
      return static_cast<C_ALIEN_OnCommit*>(callback.get())->alien_done.get_future();
    }).then([](int r){
      return seastar::now();
    });
  });
}

seastar::future<> AlienStore::write_meta(const std::string& key,
                           const std::string& value)
{
  logger().debug("{}", __func__);
  return seastar::do_with(std::move(key),std::move(value),[=]
               (const std::string& key, const std::string& value){
    return tp.submit([=, &key, &value](){
      int r = store->write_meta(key, value);
      return r;
    });
  }).then([] (int r){
    return seastar::make_ready_future<>();
  });

}

seastar::future<int, std::string>AlienStore::read_meta(const std::string& key)
{
  logger().debug("{}", __func__);
  return tp.submit([=](){
    std::string value(4096, '\0');
    int r = store->read_meta(key, &value);
    if (r >0) {
      value.resize(r);
      boost::algorithm::trim_right_if(value,
                                      [](unsigned char c) {return isspace(c);});
    } else {
      value.clear();
    }
    return std::make_pair(r, value);
  }).then([] (auto entry){
    return seastar::make_ready_future<int, std::string>(entry.first, entry.second);
  });
}

uuid_d AlienStore::get_fsid() const
{
  logger().debug("{}", __func__);
  return osd_fsid;
}

seastar::future<store_statfs_t> AlienStore::stat() const
{
  logger().info("{}", __func__);
  store_statfs_t st;
  osd_alert_list_t alerts;
  return seastar::do_with(std::move(st),std::move(alerts), [this](store_statfs_t &st, osd_alert_list_t &alerts){
    return tp.submit([this, &st, &alerts](){
      int r = store->statfs(&st, &alerts);
      return r;
    }).then([&st] (int r) {
      return seastar::make_ready_future<store_statfs_t>(std::move(st));
    });
  });
}

unsigned AlienStore::get_max_attr_name_length() const
{
  logger().info("{}", __func__);
  return 256;
}

}
