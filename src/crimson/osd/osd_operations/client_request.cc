// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/common/exception.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operation_sequencer.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ClientRequest::ClientRequest(
  OSD &osd, crimson::net::ConnectionRef conn, Ref<MOSDOp> &&m)
  : osd(osd),
    conn(conn),
    m(m),
    sequencer(get_osd_priv(conn.get()).op_sequencer[m->get_spg()])
{}

ClientRequest::~ClientRequest()
{
  logger().debug("{}: destroying", *this);
}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << "m=[" << *m << "]";
}

void ClientRequest::dump_detail(Formatter *f) const
{
  logger().debug("{}: dumping", *this);
#if 0
  std::apply([f] (auto... event) {
    (..., event.dump(f));
  }, blockers);
#endif
}

ConnectionPipeline &ClientRequest::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).client_request_conn_pipeline;
}

ConnectionPipeline &ClientRequest::cp()
{
  return get_osd_priv(conn.get()).client_request_conn_pipeline;
}

ClientRequest::PGPipeline &ClientRequest::pp(PG &pg)
{
  return pg.client_request_pg_pipeline;
}

bool ClientRequest::same_session_and_pg(const ClientRequest& other_op) const
{
  return &get_osd_priv(conn.get()) == &get_osd_priv(other_op.conn.get()) &&
         m->get_spg() == other_op.m->get_spg();
}

bool ClientRequest::is_pg_op() const
{
  return std::any_of(
    begin(m->ops), end(m->ops),
    [](auto& op) { return ceph_osd_op_type_pg(op.op.op); });
}

seastar::future<seastar::stop_iteration> ClientRequest::with_pg_int(
  ShardServices &shard_services, Ref<PG> pgref)
{
  epoch_t same_interval_since = pgref->get_interval_start_epoch();
  logger().debug("{} same_interval_since: {}", *this, same_interval_since);
  if (m->finish_decode()) {
    m->clear_payload();
  }
  return interruptor::with_interruption(
    [this, &shard_services, pgref]() mutable {
      return sequencer.start_op(
	*this, handle, shard_services.registry,
	interruptor::wrap_function([pgref, this, &shard_services] {
	  PG &pg = *pgref;
	  if (pg.can_discard_op(*m)) {
	    return osd.send_incremental_map(
	      conn, m->get_map_epoch()
	    ).then([this, &shard_services] {
	      sequencer.finish_op_out_of_order(*this, shard_services.registry);
	      return interruptor::now();
	    });
	  }
	  return enter_stage<interruptor>(pp(pg).await_map
	  ).then_interruptible([this, &pg] {
	    return with_blocking_event<
	      PG_OSDMapGate::OSDMapBlocker::BlockingEvent
	      >([this, &pg] (auto&& trigger) {
		return pg.osdmap_gate.wait_for_map(std::move(trigger),
						   m->get_min_epoch());
	      });
	  }).then_interruptible([this, &pg](auto map) {
	    return enter_stage<interruptor>(pp(pg).wait_for_active);
	  }).then_interruptible([this, &pg]() {
	    return with_blocking_event<
	      PGActivationBlocker::BlockingEvent
	      >([&pg] (auto&& trigger) {
                return pg.wait_for_active_blocker.wait(std::move(trigger));
	      });
	  }).then_interruptible(
	    [this, &shard_services, pgref=std::move(pgref)]() mutable {
	      if (is_pg_op()) {
		return process_pg_op(
		  pgref
		).then_interruptible([this, &shard_services] {
		  sequencer.finish_op_out_of_order(*this, shard_services.registry);
		});
	      } else {
		return process_op(
		  pgref
		).then_interruptible([this, &shard_services](const seq_mode_t mode) {
		  if (mode == seq_mode_t::IN_ORDER) {
		    sequencer.finish_op_in_order(*this);
		  } else {
		    assert(mode == seq_mode_t::OUT_OF_ORDER);
		    sequencer.finish_op_out_of_order(*this, shard_services.registry);
		  }
		});
	      }
	    });
	})).then_interruptible([] {
	  return seastar::stop_iteration::yes;
	});
    }, [this, pgref](std::exception_ptr eptr) {
      if (should_abort_request(*this, std::move(eptr))) {
	sequencer.abort();
	return seastar::stop_iteration::yes;
      } else {
	sequencer.maybe_reset(*this);
	return seastar::stop_iteration::no;
      }
    }, pgref);
}

seastar::future<> ClientRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  return seastar::repeat([this, &shard_services, pgref]() mutable {
    return with_pg_int(shard_services, pgref);
  });
}

ClientRequest::interruptible_future<>
ClientRequest::process_pg_op(
  Ref<PG> &pg)
{
  return pg->do_pg_ops(
    m
  ).then_interruptible([this, pg=std::move(pg)](MURef<MOSDOpReply> reply) {
    return conn->send(std::move(reply));
  });
}

ClientRequest::interruptible_future<ClientRequest::seq_mode_t>
ClientRequest::process_op(Ref<PG> &pg)
{
  return enter_stage<interruptor>(
    pp(*pg).recover_missing
  ).then_interruptible(
    [this, pg]() mutable {
    return do_recover_missing(pg, m->get_hobj());
  }).then_interruptible([this, pg]() mutable {
    return pg->already_complete(m->get_reqid()).then_unpack_interruptible(
      [this, pg](bool completed, int ret) mutable
      -> PG::load_obc_iertr::future<seq_mode_t> {
      if (completed) {
        auto reply = crimson::make_message<MOSDOpReply>(
          m.get(), ret, pg->get_osdmap_epoch(),
          CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, false);
        return conn->send(std::move(reply)).then([] {
          return seastar::make_ready_future<seq_mode_t>(seq_mode_t::OUT_OF_ORDER);
        });
      } else {
        return enter_stage<interruptor>(pp(*pg).get_obc).then_interruptible(
          [this, pg]() mutable -> PG::load_obc_iertr::future<seq_mode_t> {
          logger().debug("{}: got obc lock", *this);
          op_info.set_from_op(&*m, *pg->get_osdmap());
          // XXX: `do_with()` is just a workaround for `with_obc_func_t` imposing
          // `future<void>`.
          return seastar::do_with(seq_mode_t{}, [this, &pg] (seq_mode_t& mode) {
            return pg->with_locked_obc(m->get_hobj(), op_info,
                                       [this, pg, &mode](auto obc) mutable {
              return enter_stage<interruptor>(pp(*pg).process).then_interruptible(
              [this, pg, obc, &mode]() mutable {
                return do_process(pg, obc).then_interruptible([&mode] (seq_mode_t _mode) {
                  mode = _mode;
                  return seastar::now();
                });
              });
            }).safe_then_interruptible([&mode] {
              return PG::load_obc_iertr::make_ready_future<seq_mode_t>(mode);
            });
          });
        });
      }
    });
  }).safe_then_interruptible([pg=std::move(pg)] (const seq_mode_t mode) {
    return seastar::make_ready_future<seq_mode_t>(mode);
  }, PG::load_obc_ertr::all_same_way([](auto &code) {
    logger().error("ClientRequest saw error code {}", code);
    return seastar::make_ready_future<seq_mode_t>(seq_mode_t::OUT_OF_ORDER);
  }));
}

ClientRequest::interruptible_future<ClientRequest::seq_mode_t>
ClientRequest::do_process(Ref<PG>& pg, crimson::osd::ObjectContextRef obc)
{
  if (!pg->is_primary()) {
    // primary can handle both normal ops and balanced reads
    if (is_misdirected(*pg)) {
      logger().trace("do_process: dropping misdirected op");
      return seastar::make_ready_future<seq_mode_t>(seq_mode_t::OUT_OF_ORDER);
    } else if (const hobject_t& hoid = m->get_hobj();
               !pg->get_peering_state().can_serve_replica_read(hoid)) {
      auto reply = crimson::make_message<MOSDOpReply>(
	m.get(), -EAGAIN, pg->get_osdmap_epoch(),
	m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK),
	!m->has_flag(CEPH_OSD_FLAG_RETURNVEC));
      return conn->send(std::move(reply)).then([] {
        return seastar::make_ready_future<seq_mode_t>(seq_mode_t::OUT_OF_ORDER);
      });
    }
  }
  return pg->do_osd_ops(m, obc, op_info).safe_then_unpack_interruptible(
    [this, pg](auto submitted, auto all_completed) mutable {
    return submitted.then_interruptible([this, pg] {
      return enter_stage<interruptor>(pp(*pg).wait_repop);
    }).then_interruptible(
      [this, pg, all_completed=std::move(all_completed)]() mutable {
      return all_completed.safe_then_interruptible(
        [this, pg](MURef<MOSDOpReply> reply) {
        return enter_stage<interruptor>(pp(*pg).send_reply).then_interruptible(
          [this, reply=std::move(reply)]() mutable {
          return conn->send(std::move(reply)).then([] {
            return seastar::make_ready_future<seq_mode_t>(seq_mode_t::IN_ORDER);
          });
        });
      }, crimson::ct_error::eagain::handle([this, pg]() mutable {
        return process_op(pg);
      }));
    });
  }, crimson::ct_error::eagain::handle([this, pg]() mutable {
    return process_op(pg);
  }));
}

bool ClientRequest::is_misdirected(const PG& pg) const
{
  // otherwise take a closer look
  if (const int flags = m->get_flags();
      flags & CEPH_OSD_FLAG_BALANCE_READS ||
      flags & CEPH_OSD_FLAG_LOCALIZE_READS) {
    if (!op_info.may_read()) {
      // no read found, so it can't be balanced read
      return true;
    }
    if (op_info.may_write() || op_info.may_cache()) {
      // write op, but i am not primary
      return true;
    }
    // balanced reads; any replica will do
    return pg.is_nonprimary();
  }
  // neither balanced nor localize reads
  return true;
}

}
