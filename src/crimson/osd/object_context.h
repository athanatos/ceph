// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_future.hh>

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/intrusive_lru.h"
#include "osd/object_state.h"
#include "crimson/common/config_proxy.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

template <typename OBC>
struct obc_to_hoid {
  using type = hobject_t;
  const type &operator()(const OBC &obc) {
    return obc.obs.oi.soid;
  }
};

template <typename SSC>
struct ssc_to_hoid {
  using type = hobject_t;
  const type &operator()(const SSC &ssc) {
    return ssc.oid;
  }
};

class SnapSetContext : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    hobject_t, SnapSetContext, ssc_to_hoid<SnapSetContext>>> {
public:
  bool loaded : 1;
  bool exists : 1;

  hobject_t oid;
  SnapSet snapset;

  explicit SnapSetContext(const hobject_t& o) :
    loaded(false), exists(false), oid(o) { }

  void set_snapset(SnapSet &&ss) {
    snapset = std::move(ss);
    loaded = true;
  }
};
using SnapSetContextRef = SnapSetContext::Ref;

class ObjectContext : public Blocker,
		      public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    hobject_t, ObjectContext, obc_to_hoid<ObjectContext>>>
{
public:
  bool loaded : 1;
  ObjectState obs;
  SnapSetContextRef ssc;
  ObjectContext(const hobject_t &hoid) : loaded(false) {}

  void set_obs_ssc(ObjectState &&obs, SnapSetContextRef &&ssc) {
    obs = std::move(obs);
    ssc = std::move(ssc);
    loaded = true;
  }

private:
  RWState rwstate;
  seastar::shared_mutex wait_queue;
  std::optional<seastar::shared_promise<>> wake;

  template <typename F>
  seastar::future<> with_queue(F &&f) {
    return wait_queue.lock().then([this, f=std::move(f)] {
      ceph_assert(!wake);
      return seastar::repeat([this, f=std::move(f)]() {
	if (f()) {
	  wait_queue.unlock();
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
	} else {
	  rwstate.inc_waiters();
	  wake = seastar::shared_promise<>();
	  return (*wake).get_shared_future().then([this, f=std::move(f)] {
	    wake = std::nullopt;
	    rwstate.dec_waiters(1);
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no);
	  });
	}
      });
    });
  }


  const char *get_type_name() const final {
    return "ObjectContext";
  }
  void dump_detail(Formatter *f) const final;

  template <typename LockF>
  seastar::future<> get_lock(
    Operation *op,
    LockF &&lockf) {
    return op->with_blocking_future(
      make_blocking_future(with_queue(std::forward<LockF>(lockf))));
  }

  template <typename UnlockF>
  void put_lock(
    UnlockF &&unlockf) {
    if (unlockf() && wake) (*wake).set_value();
  }
public:
  seastar::future<> get_lock_type(Operation *op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_lock(op, [this] { return rwstate.get_write_lock(); });
    case RWState::RWREAD:
      return get_lock(op, [this] { return rwstate.get_read_lock(); });
    case RWState::RWEXCL:
      return get_lock(op, [this] { return rwstate.get_excl_lock(); });
    case RWState::RWNONE:
      return seastar::now();
    default:
      ceph_abort_msg("invalid lock type");
      return seastar::now();
    }
  }

  void put_lock_type(RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return put_lock([this] { return rwstate.put_write(); });
    case RWState::RWREAD:
      return put_lock([this] { return rwstate.put_read(); });
    case RWState::RWEXCL:
      return put_lock([this] { return rwstate.put_excl(); });
    case RWState::RWNONE:
      return;
    default:
      ceph_abort_msg("invalid lock type");
      return;
    }
  }

  void degrade_excl_to(RWState::State type) {
    // assume we already hold an excl lock
    bool put = rwstate.put_excl();
    bool success = false;
    switch (type) {
    case RWState::RWWRITE:
      success = rwstate.get_write_lock();
      break;
    case RWState::RWREAD:
      success = rwstate.get_read_lock();
      break;
    case RWState::RWEXCL:
      success = rwstate.get_excl_lock();
      break;
    case RWState::RWNONE:
      success = true;
      break;
    default:
      ceph_abort_msg("invalid lock type");
      break;
    }
    ceph_assert(success);
    if (put && wake) {
      (*wake).set_value();
    }
  }

  bool empty() const { return rwstate.empty(); }

  template <typename F>
  seastar::future<> get_write_greedy(Operation *op) {
    return get_lock(op, [this] { return rwstate.get_write_lock(true); });
  }

  bool try_get_read_lock() {
    return rwstate.get_read_lock();
  }
  void drop_read() {
    return put_lock_type(RWState::RWREAD);
  }
  bool get_recovery_read() {
    return rwstate.get_recovery_read();
  }
  void drop_recovery_read() {
    ceph_assert(rwstate.recovery_read_marker);
    drop_read();
    rwstate.recovery_read_marker = false;
  }
  bool maybe_get_excl() {
    return rwstate.get_excl_lock();
  }

  bool is_request_pending() {
    return !rwstate.empty();
  }
};
using ObjectContextRef = ObjectContext::Ref;

class ObjectContextRegistry : public md_config_obs_t  {
  ObjectContext::lru_t obc_lru;
  SnapSetContext::lru_t ssc_lru;

public:
  ObjectContextRegistry(crimson::common::ConfigProxy &conf);

  std::pair<ObjectContextRef, bool> get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get_or_create(hoid);
  }

  std::pair<SnapSetContextRef, bool> get_cached_ssc(const hobject_t &hoid) {
    return ssc_lru.get_or_create(hoid);
  }

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
};

}
