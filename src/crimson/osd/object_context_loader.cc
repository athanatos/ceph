#include "crimson/common/coroutine.h"
#include "crimson/osd/object_context_loader.h"
#include "osd/osd_types_fmt.h"
#include "osd/object_state_fmt.h"

SET_SUBSYS(osd);

namespace crimson::osd {

using crimson::common::local_conf;

  ObjectContextLoader::load_and_lock_fut
  ObjectContextLoader::load_and_lock(Manager &manager, RWState::State lock_type)
  {
    LOG_PREFIX(ObjectContextLoader::load_and_lock);
    auto releaser = manager.get_releaser();
    if (manager.head_state.is_empty() &&
	!manager.target.is_head() &&
	(!manager.options.clone_only ||
	 manager.options.resolve_clone)) {
      auto [obc, existed] = obc_registry.get_cached_obc(manager.target.get_head());
      manager.head_state.obc = obc;
      manager.head_state.obc->append_to(obc_set_accessing);

      if (!existed) {
	bool locked = obc->lock.try_lock_for_excl();
	ceph_assert(locked);
	manager.head_state.state = RWState::RWEXCL;

	co_await load_obc(obc);
	
	obc->lock.demote_to_read();
	manager.head_state.state = RWState::RWREAD;
      } else {
	co_await interruptor::make_interruptible(
	  manager.head_state.obc->lock.lock_for_read());
	manager.head_state.state = RWState::RWREAD;
      }
    }

    if (!manager.target.is_head() && manager.options.resolve_clone) {
      ceph_assert(!manager.head_state.is_empty());
      auto resolved_oid = resolve_oid(
	manager.head_state.obc->get_head_ss(),
	manager.target);
      if (!resolved_oid) {
	ERRORDPP("clone {} not found", dpp, manager.target);
	co_await load_obc_iertr::future<>(
	  crimson::ct_error::enoent::make()
	);
      }
      manager.target = *resolved_oid;
    }

    auto [obc, existed] = obc_registry.get_cached_obc(manager.target);
    manager.target_state.obc = obc;
    manager.target_state.obc->append_to(obc_set_accessing);

    if (!existed) {
      bool locked = obc->lock.try_lock_for_excl();
      ceph_assert(locked);
      manager.target_state.state = RWState::RWEXCL;
      
      co_await load_obc(obc);
	
      switch (lock_type) {
      case RWState::RWWRITE:
	obc->lock.demote_to_write();
	manager.target_state.state = RWState::RWWRITE;
	break;
      case RWState::RWREAD:
	obc->lock.demote_to_read();
	manager.target_state.state = RWState::RWREAD;
	break;
      case RWState::RWNONE:
	obc->lock.unlock_for_excl();
	manager.target_state.state = RWState::RWNONE;
	break;
      case RWState::RWEXCL:
	//noop
	break;
      default:
	ceph_assert(0 == "impossible");
      }
    } else {
      switch (lock_type) {
      case RWState::RWWRITE:
	co_await interruptor::make_interruptible(
	  manager.target_state.obc->lock.lock_for_write());
	manager.target_state.state = RWState::RWWRITE;
	break;
      case RWState::RWREAD:
	co_await interruptor::make_interruptible(
	  manager.target_state.obc->lock.lock_for_read());
	manager.target_state.state = RWState::RWREAD;
	break;
      case RWState::RWNONE:
	// noop
	break;
      case RWState::RWEXCL:
	co_await interruptor::make_interruptible(
	  manager.target_state.obc->lock.lock_for_excl());
	manager.target_state.state = RWState::RWEXCL;
	break;
      default:
	ceph_assert(0 == "impossible");
      }
    }

    if (manager.target.is_head() && manager.head_state.is_empty()) {
      manager.head_state.obc = manager.target_state.obc;
      manager.head_state.obc->append_to(obc_set_accessing);
    }
    releaser.cancel();
  }

  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::load_obc(ObjectContextRef obc)
  {
    LOG_PREFIX(ObjectContextLoader::load_obc);
    return backend.load_metadata(obc->get_oid())
    .safe_then_interruptible(
      [FNAME, this, obc=std::move(obc)](auto md)
      -> load_obc_ertr::future<> {
      const hobject_t& oid = md->os.oi.soid;
      DEBUGDPP("loaded obs {} for {}", dpp, md->os.oi, oid);
      if (oid.is_head()) {
        if (!md->ssc) {
	  ERRORDPP("oid {} missing snapsetcontext", dpp, oid);
          return crimson::ct_error::object_corrupted::make();
        }
        obc->set_head_state(std::move(md->os),
                            std::move(md->ssc));
      } else {
        // we load and set the ssc only for head obc.
        // For clones, the head's ssc will be referenced later.
        // See set_clone_ssc
        obc->set_clone_state(std::move(md->os));
      }
      DEBUGDPP("loaded obc {} for {}", dpp, obc->obs.oi, obc->obs.oi.soid);
      return seastar::now();
    });
  }

  void ObjectContextLoader::notify_on_change(bool is_primary)
  {
    LOG_PREFIX(ObjectContextLoader::notify_on_change);
    DEBUGDPP("is_primary: {}", dpp, is_primary);
    for (auto& obc : obc_set_accessing) {
      DEBUGDPP("interrupting obc: {}", dpp, obc.get_oid());
      obc.interrupt(::crimson::common::actingset_changed(is_primary));
    }
  }
}
