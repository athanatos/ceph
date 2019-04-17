// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg.h"

#include <functional>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"

#include "osd/OSDMap.h"

#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_store.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/pg_meta.h"

#include "pg_backend.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    return {new Message{std::forward<Args>(args)...}, false};
  }
}

PG::PG(spg_t pgid,
       pg_shard_t pg_shard,
       pg_pool_t&& pool,
       std::string&& name,
       std::unique_ptr<PGBackend> backend,
       cached_map_t osdmap,
       ceph::net::Messenger& msgr)
  : peering_state(
      nullptr /* cct TODO */,
      pg_shard,
      pgid,
      PGPool(
	nullptr /* cct TODO */,
	osdmap,
	pgid.pool(),
	pool,
	osdmap->get_pool_name(pgid.pool())),
      osdmap,
      this,
      this),
    backend{std::move(backend)},
    osdmap{osdmap},
    msgr{msgr}
{
  // TODO
}

seastar::future<> PG::read_state(ceph::os::CyanStore* store)
{
  return seastar::now();
#if 0
  PGMeta{store, pgid}.load().then(
    [this](pg_info_t pg_info_, PastIntervals past_intervals_) {
      info = std::move(pg_info_);
      last_written_info = info;
      past_intervals = std::move(past_intervals_);
      // initialize current mapping
      {
        vector<int> new_up, new_acting;
        int new_up_primary, new_acting_primary;
        osdmap->pg_to_up_acting_osds(pgid.pgid,
                                     &new_up, &new_up_primary,
                                     &new_acting, &new_acting_primary);
        update_primary_state(new_up, new_up_primary,
                             new_acting, new_acting_primary);
      }
      info.stats.up = up;
      info.stats.up_primary = up_primary.osd;
      info.stats.acting = acting;
      info.stats.acting_primary = primary.osd;
      info.stats.mapping_epoch = info.history.same_interval_since;
      peering_state.handle_event(Initialize{});
      // note: we don't activate here because we know the OSD will advance maps
      // during boot.
      return seastar::now();
    });
#endif
}

seastar::future<> PG::do_peering_event(std::unique_ptr<PGPeeringEvent> evt)
{
  return seastar::now();
}

seastar::future<> PG::handle_advance_map(cached_map_t next_map)
{
  return seastar::now();
}

seastar::future<> PG::handle_activate_map()
{
  return seastar::now();
}

seastar::future<> PG::dispatch_context(PeeringState::PeeringCtx &&ctx)
{
  return seastar::now();
#if 0
  return seastar::do_with(recovery::Context{ctx}, [this](auto& todo) {
    return seastar::when_all_succeed(
      seastar::parallel_for_each(std::move(todo.notifies),
        [this](auto& osd_notifies) {
          auto& [peer, notifies] = osd_notifies;
          auto m = make_message<MOSDPGNotify>(get_osdmap_epoch(),
                                              std::move(notifies));
          return send_to_osd(peer, m, get_osdmap_epoch());
        }),
      seastar::parallel_for_each(std::move(todo.queries),
        [this](auto& osd_queries) {
          auto& [peer, queries] = osd_queries;
          auto m = make_message<MOSDPGQuery>(get_osdmap_epoch(),
                                             std::move(queries));
          return send_to_osd(peer, m, get_osdmap_epoch());
        }),
      seastar::parallel_for_each(std::move(todo.infos),
        [this](auto& osd_infos) {
          auto& [peer, infos] = osd_infos;
          auto m = make_message<MOSDPGInfo>(get_osdmap_epoch(),
                                            std::move(infos));
          return send_to_osd(peer, m, get_osdmap_epoch());
        })
    );
  });
#endif
}

void PG::print(ostream& out) const
{
  out << peering_state;
}


std::ostream& operator<<(std::ostream& os, const PG& pg)
{
  pg.print(os);
  return os;
}

seastar::future<> PG::send_to_osd(int peer, Ref<Message> m, epoch_t from_epoch)
{
  if (osdmap->is_down(peer) || osdmap->get_info(peer).up_from > from_epoch) {
    return seastar::now();
  } else {
    return msgr.connect(osdmap->get_cluster_addrs(peer).front(),
                        CEPH_ENTITY_TYPE_OSD)
     .then([m, this] (auto xconn) {
       return (*xconn)->send(m);
     });
  }
}

seastar::future<> PG::wait_for_active()
{
  logger().debug("wait_for_active: {}", peering_state.get_pg_state_string());
  if (peering_state.is_active()) {
    return seastar::now();
  } else {
    if (!active_promise) {
      active_promise = seastar::shared_promise<>();
    }
    return active_promise->get_shared_future();
  }
}

seastar::future<>
PG::do_osd_op(const object_info_t& oi, OSDOp* osd_op)
{
  switch (const auto& op = osd_op->op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return backend->read(oi,
                         op.extent.offset,
                         op.extent.length,
                         op.extent.truncate_size,
                         op.extent.truncate_seq,
                         op.flags).then([osd_op](bufferlist bl) {
      osd_op->rval = bl.length();
      osd_op->outdata = std::move(bl);
      return seastar::now();
    });
  default:
    return seastar::now();
  }
}

seastar::future<Ref<MOSDOpReply>> PG::do_osd_ops(Ref<MOSDOp> m)
{
  // todo: issue requests in parallel if they don't write,
  // with writes being basically a synchronization barrier
  return seastar::do_with(std::move(m), [this](auto& m) {
    return seastar::do_for_each(begin(m->ops), end(m->ops),
                                [m,this](OSDOp& osd_op) {
      const auto oid = (m->get_snapid() == CEPH_SNAPDIR ?
                        m->get_hobj().get_head() :
                        m->get_hobj());
      return backend->get_object(oid).then([&osd_op,this](auto oi) {
        return do_osd_op(*oi, &osd_op);
      }).handle_exception_type([&osd_op](const object_not_found&) {
        osd_op.rval = -ENOENT;
        throw;
      });
    }).then([=] {
      auto reply = make_message<MOSDOpReply>(m.get(), 0, get_osdmap_epoch(),
                                             0, false);
      reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    }).handle_exception_type([=](const object_not_found& dne) {
      auto reply = make_message<MOSDOpReply>(m.get(), -ENOENT, get_osdmap_epoch(),
                                             0, false);
      reply->set_enoent_reply_versions(peering_state.get_info().last_update,
                                       peering_state.get_info().last_user_version);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    });
  });
}

seastar::future<> PG::handle_op(ceph::net::ConnectionRef conn,
                                Ref<MOSDOp> m)
{
  return wait_for_active().then([conn, m, this] {
    if (m->finish_decode()) {
      m->clear_payload();
    }
    return do_osd_ops(m);
  }).then([conn](Ref<MOSDOpReply> reply) {
    return conn->send(reply);
  });
}
