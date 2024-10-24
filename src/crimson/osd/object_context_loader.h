#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/defer.hh>
#include "crimson/common/errorator.h"
#include "crimson/common/log.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg_backend.h"
#include "osd/object_state_fmt.h"

namespace crimson::osd {
class ObjectContextLoader {
public:
  using obc_accessing_list_t = boost::intrusive::list<
    ObjectContext,
    ObjectContext::obc_accessing_option_t>;

  ObjectContextLoader(
    ObjectContextRegistry& _obc_services,
    PGBackend& _backend,
    DoutPrefixProvider& dpp)
    : obc_registry{_obc_services},
      backend{_backend},
      dpp{dpp}
    {}

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;

  class Manager {
    ObjectContextLoader &loader;
    hobject_t target;

    Manager() = delete;
    template <typename T>
    Manager(ObjectContextLoader &loader, T &&t)
      : loader(loader), target(std::forward<T>(t)) {}
    Manager(const Manager &) = delete;
    Manager &operator=(const Manager &o) = delete;

    struct options_t {
      bool resolve_clone = true;
      bool clone_only = false;
    } options;

    struct state_t {
      RWState::State state = RWState::RWNONE;
      ObjectContextRef obc;
      bool is_empty() const { return !obc; }
    };

    ObjectContextRef orderer_obc;
    state_t head_state;
    state_t target_state;

    friend ObjectContextLoader;

    void release_state(state_t &s) {
      LOG_PREFIX(ObjectContextLoader::release_state);
      if (s.is_empty()) return;

      switch (s.state) {
      case RWState::RWREAD:
	s.obc->lock.unlock_for_read();
	break;
      case RWState::RWWRITE:
	s.obc->lock.unlock_for_write();
	break;
      case RWState::RWEXCL:
	s.obc->lock.unlock_for_excl();
	break;
      case RWState::RWNONE:
	// noop
	break;
      default:
	ceph_assert(0 == "invalid");
      }

      SUBDEBUGDPP(
	osd, "released object {}, {}",
	loader.dpp, s.obc->get_oid(), s.obc->obs);
      s.obc->remove_from(loader.obc_set_accessing);
      s = state_t();
    }
  public:
    Manager(Manager &&rhs) : loader(rhs.loader) {
      std::swap(target, rhs.target);
      std::swap(options, rhs.options);
      std::swap(head_state, rhs.head_state);
      std::swap(target_state, rhs.target_state);
    }

    Manager &operator=(Manager &&o) {
      this->~Manager();
      new(this) Manager(std::move(o));
      return *this;
    }

    ObjectContextRef &get_obc() {
      ceph_assert(!target_state.is_empty());
      ceph_assert(!target_state.obc->is_loaded());
      return target_state.obc;
    }

    ObjectContextRef &get_head_obc() {
      ceph_assert(!head_state.is_empty());
      ceph_assert(!head_state.obc->is_loaded());
      return head_state.obc;
    }

    void release() {
      orderer_obc = ObjectContextRef{};
      release_state(head_state);
      release_state(target_state);
    }

    auto get_releaser() {
      return seastar::defer([this] {
	release();
      });
    }

    CommonOBCPipeline &obc_pp() {
      return orderer_obc->obc_pipeline;
    }

    ~Manager() {
      release();
    }
  };
  Manager get_obc_manager(hobject_t oid, bool resolve_clone = true) {
    Manager ret(*this, oid);
    ret.options.resolve_clone = resolve_clone;
    std::tie(ret.orderer_obc, std::ignore) = obc_registry.get_cached_obc(oid.get_head());
    return ret;
  }

  using load_and_lock_ertr = load_obc_ertr;
  using load_and_lock_iertr = interruptible::interruptible_errorator<
    IOInterruptCondition, load_and_lock_ertr>;
  using load_and_lock_fut = load_and_lock_iertr::future<>;
  load_and_lock_fut load_and_lock(Manager &, RWState::State);

  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;

  using with_obc_func_t =
    std::function<load_obc_iertr::future<> (ObjectContextRef, ObjectContextRef)>;

  // Use this variant by default
  // If oid is a clone object, the clone obc *and* it's
  // matching head obc will be locked and can be used in func.
  // resolve_clone: For SnapTrim, it may be possible that it
  //                won't be possible to resolve the clone.
  // See SnapTrimObjSubEvent::remove_or_update - in_removed_snaps_queue usage.
  template<RWState::State State>
  load_obc_iertr::future<> with_obc(hobject_t oid,
                                    with_obc_func_t func,
                                    bool resolve_clone = true) {
    auto manager = get_obc_manager(oid, resolve_clone);
    co_await load_and_lock(manager, State);
    co_await std::invoke(
      func, manager.get_head_obc(), manager.get_obc());
  }

  // Use this variant in the case where the head object
  // obc is already locked and only the clone obc is needed.
  // Avoid nesting with_head_obc() calls by using with_clone_obc()
  // with an already locked head.
  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc_only(ObjectContextRef head,
                                               hobject_t clone_oid,
                                               with_obc_func_t func,
                                               bool resolve_clone = true) {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc_only);
    SUBDEBUGDPP(osd, "{}", dpp, clone_oid);
    auto manager = get_obc_manager(clone_oid, resolve_clone);
    manager.head_state.obc = head;
    manager.head_state.obc->append_to(obc_set_accessing);
    manager.options.clone_only = true;
    co_await load_and_lock(manager, State);
    co_await std::invoke(func, head, manager.get_obc());
  }

  void notify_on_change(bool is_primary);

private:
  ObjectContextRegistry& obc_registry;
  PGBackend& backend;
  DoutPrefixProvider& dpp;
  obc_accessing_list_t obc_set_accessing;

  load_obc_iertr::future<> load_obc(ObjectContextRef obc);
};

using ObjectContextManager = ObjectContextLoader::Manager;

}
