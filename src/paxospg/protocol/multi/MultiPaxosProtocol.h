// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_PAXOSPG_SIMPLE_PAXOS_H
#define CEPH_PAXOSPG_SIMPLE_PAXOS_H

#include <limits>
#include <memory>
#include <mutex>
#include <boost/intrusive/set.hpp>
#include <boost/container/static_vector.hpp>
#include <algorithm>

#include "include/assert.h"
#include "common/hobject.h"
#include "messages/MSPPGRequest.h"
#include "messages/MSPPGResponse.h"
#include "paxospg/Consensus.h"
#include "paxospg/types.h"
#include "paxospg/protocol/multi/msg_types.h"

namespace PaxosPG {
namespace Protocol {
namespace MultiPaxos {

// TODO: figure out where to put the config stuff
// TODO: Must always be larger than pg_bits
const unsigned PARTITION_BITS = 3;
const unsigned MAX_ACCEPTORS = 16;

class config_t {
  const unsigned total_acceptors;
  const unsigned min_to_reconstruct;
  const unsigned min_q2;
public:
  config_t(
    unsigned total_acceptors,
    unsigned min_to_reconstruct,
    unsigned min_q2)
    : total_acceptors(total_acceptors), min_to_reconstruct(min_to_reconstruct),
      min_q2(min_q2) {}
  unsigned get_total_acceptors() const { return total_acceptors; };
  unsigned get_min_to_commit() const { return min_to_reconstruct; };
  unsigned get_min_q1() const { return total_acceptors - min_q2 + 1; }
  unsigned get_min_q2() const { return min_q2; }
};

template <typename P>
class ClientPartition :
    private ClusterMap::Listener {
  using Processor = P;
  using CommandController = typename P::CommandController;
  using Command = typename CommandController::Command;
  using CommandMetadata = typename CommandController::CommandMetadata;
  using CommandRankedEncoding = typename CommandController::CommandRankedEncoding;
  using Response = typename CommandController::Response;
  using ResponseRankedEncoding = typename CommandController::ResponseRankedEncoding;

  PaxosPG::Environment::Ref env;
  config_t conf;
  CommandController cc;
  ClusterMap::Ref curmap;

  struct InProgressOp : boost::intrusive::set_base_hook<> {
    epoch_t epoch = 0;
    ceph_tid_t tid;
    hobject_t hoid;
    Command c;
    Ceph::Promise<Response> on_complete;

    pg_t pgid;
    boost::container::static_vector<osd_id_t, MAX_ACCEPTORS> mapping;

    struct ConnectionStatus : boost::intrusive::set_base_hook<> {
      static const uint8_t DISCONNECTED = 0xFF;
      uint8_t rank = DISCONNECTED;
      bool heard_from = false;
      ConnectionRef conn;
      boost::intrusive_ptr<MSPPGRequest> to_send;

      ConnectionStatus() : rank(0xFF), heard_from(false) {}

      const InProgressOp &get_in_progress_op() const {
	return *reinterpret_cast<const InProgressOp*>(
	  reinterpret_cast<const char*>(this - rank) -
	  offsetof(InProgressOp, connections));
      }

      InProgressOp &get_in_progress_op() {
	return *reinterpret_cast<InProgressOp*>(
	  reinterpret_cast<char*>(this - rank) -
	  offsetof(InProgressOp, connections));
      }


      friend bool operator==(
	const ConnectionStatus &lhs, const ConnectionStatus &rhs) {
	return (&*(lhs.conn) == &*(rhs.conn)) && 
	  (lhs.get_in_progress_op().tid == rhs.get_in_progress_op().tid);
      }
      friend bool operator<(
	const ConnectionStatus &lhs, const ConnectionStatus &rhs) {
	return (&*(lhs.conn) < &*(rhs.conn)) || (
	  (&*(lhs.conn) == &*(rhs.conn)) &&
	  (lhs.get_in_progress_op().tid < rhs.get_in_progress_op().tid));
      }
    };
    ConnectionStatus connections[MAX_ACCEPTORS];

    InProgressOp(
      ceph_tid_t tid,
      hobject_t hoid,
      Command c,
      Ceph::Promise<Response> on_complete)
      : tid(tid), hoid(hoid), c(std::move(c)),
	on_complete(std::move(on_complete)) {}

    friend bool operator==(
      const InProgressOp &lhs, const InProgressOp &rhs) {
      return lhs.tid == rhs.tid;
    }
    friend bool operator<(
      const InProgressOp &lhs, const InProgressOp &rhs) {
      return lhs.tid < rhs.tid;
    }
  };
  using InProgressOpMap = boost::intrusive::set<InProgressOp>;
  InProgressOpMap in_progress_ops_by_tid;

  typename InProgressOpMap::iterator &get_op_by_tid(ceph_tid_t tid) {
    static const struct {
      bool operator()(ceph_tid_t tid, const InProgressOp &op) {
	return tid < op.tid;
      }
    } comp;
    return in_progress_ops_by_tid.find(tid, comp);
  }

  using ConnectionStatusMap = boost::intrusive::multiset<
    typename InProgressOp::ConnectionStatus>;
  ConnectionStatusMap connection_status_map;

  std::pair<typename ConnectionStatusMap::iterator,
	    typename ConnectionStatusMap::iterator>
  get_op_range_by_conn(Connection *conn) {
    static const struct {
      bool operator()(
	const typename InProgressOp::ConnectionStatus &cs,
	const std::pair<Connection *, ceph_tid_t> &right) const {
	ceph_tid_t tid = cs.get_in_progress_op().tid;
	return cs.conn.get() < right.first || ((right.first == cs.conn.get()) &&
	  tid < right.second);
      }
    } comp;
    return make_pair(
      connection_status_map.lower_bound(make_pair(conn, 0), comp),
      connection_status_map.lower_bound(
	make_pair(
	  conn, std::numeric_limits<ceph_tid_t>::max()), comp));
  }

  void disconnect_rank(typename InProgressOp::ConnectionStatus &cs) {
    if (cs.rank != InProgressOp::ConnectionStatus::DISCONNECTED) {
      connection_status_map.erase(
	connection_status_map.find(cs));
      cs.heard_from = false;
      cs.rank = InProgressOp::ConnectionStatus::DISCONNECTED;
      cs.conn = ConnectionRef();
    }
  }

  void restart_rank(
    InProgressOp &op,
    uint8_t rank,
    osd_id_t osd) {
    auto &cs = op.connections[rank];
    disconnect_rank(cs);
    ConnectionRef conn = env->get_connection(
      env->get_inst(osd));
    cs.to_send->pgid = spg_t(op.pgid, shard_id_t(rank));
    cs.to_send->epoch = op.epoch;
    cs.rank = rank;
    cs.conn = conn;
    connection_status_map.insert(cs);
    conn->send_message(cs.to_send);
  }

  void restart_op(InProgressOp &op) {
    op.mapping.clear();
    op.pgid = curmap->get_pg(op.hoid);
    op.mapping.resize(conf.get_total_acceptors(), osd_id_t::NONE);
    curmap->get_mapping(
      op.pgid,
      op.mapping.data(),
      op.mapping.size());
    ceph_assert(op.mapping.size() == conf.get_total_acceptors());
    for (uint8_t i = 0; i < conf.get_total_acceptors(); ++i) {
      restart_rank(op, i, op.mapping[i]);
    }
  }

public:
  ClientPartition(
    CommandController cc,
    PaxosPG::Environment::Ref env,
    config_t conf,
    ClusterMap::Ref initial_map)
    : env(std::move(env)),
      conf(conf),
      cc(cc),
      curmap(std::move(initial_map)) {
    env->get_cluster_map_source().get_maps_from(
      initial_map->get_epoch() + 1,
      this);
  }
  
  ~ClientPartition() {
    env->get_cluster_map_source().unregister_listener(
      this);
  }

  Ceph::Future<Response> submit_command(Command c) {
    Ceph::Future<Response> ret;
    Ceph::Future<> to_schedule;
    env->submit_task(
      this,
      to_schedule.then_ignore_error(
	[this,
	 c{std::move(c)},
	 promise{ret.get_promise()}]() mutable {
	  InProgressOp *op = new InProgressOp(
	    env->get_tid(),
	    cc.get_metadata(c).get_target(),
	    std::move(c),
	    std::move(promise));

	  CommandRankedEncoding commands[conf.get_total_acceptors()];
	  cc.get_ranked_encoding(
	    op->c,
	    conf.get_total_acceptors(),
	    nullptr, /* generate all of the ranks */
	    commands);
	  for (unsigned i = 0; i < conf.get_total_acceptors(); ++i) {
	    bufferlist bl;
	    ::encode(commands[i], bl);
	    op->connections[i].to_send = new MSPPGRequest(
	      op->tid, bl);
	  }

	  in_progress_ops_by_tid.insert(*op);
	  restart_op(*op);
	}));
    return ret;
  }

  void next_quorum(ClusterMap::Ref map) override {
    curmap = map;
    for (auto op = in_progress_ops_by_tid.begin();
	 op != in_progress_ops_by_tid.end();
	 ++op) {
      osd_id_t newmapping[conf.get_total_acceptors()];
      pg_t pgid = curmap->get_pg(op->hoid);
      if (pgid != op->pgid) {
	restart_op(*op);
	continue;
      }
      curmap->get_mapping(
	pgid,
	newmapping,
	conf.get_total_acceptors());
      if (!std::equal(
	  op->mapping.data(), op->mapping.data() + conf.get_total_acceptors(),
	  newmapping, newmapping + conf.get_total_acceptors(),
	  std::equal_to<osd_id_t>())) {
	restart_op(*op);
	continue;
      }
    }
  }

  void deliver_message(boost::intrusive_ptr<MSPPGResponse> response) {
  }

  void connection_reset(Connection *con) {
    auto range = get_op_range_by_conn(con);
    for (auto connstat = range.first; connstat != range.second;) {
      auto &cs = *(connstat++);
      restart_rank(
	cs.get_in_progress_op(),
	cs.rank,
	cs.get_in_progress_op().mapping[cs.rank]);
    }
  }
};

template <typename P>
class Server : public PaxosPG::Consensus::Server<P> {
  using Processor = P;
  using CommandController = typename P::CommandController;
  using Command = typename CommandController::Command;
  using CommandMetadata = typename CommandController::CommandMetadata;
  using CommandRankedEncoding = typename CommandController::CommandRankedEncoding;
  using Response = typename CommandController::Response;
  using ResponseRankedEncoding = typename CommandController::ResponseRankedEncoding;

  PaxosPG::Environment::Ref env;
  config_t conf;
public:
  Server(config_t conf, PaxosPG::Environment::Ref env)
    : env(std::move(env)), conf(conf) {}
};

template <typename P>
class MultiPaxosProtocol : public PaxosPG::Consensus::Protocol<P> {
  using Processor = P;
  using CommandController = typename P::CommandController;
  using Command = typename CommandController::Command;
  using Response = typename CommandController::Response;

  config_t conf;

  class Client :
    public Consensus::Client<P>, private Environment::MessageListener {
    vector<ClientPartition<P>> op_target_partitions;
    CommandController cc;
    Environment::Ref env;

  public:
    template <typename... Args>
    Client(
      CommandController cc,
      Environment::Ref env,
      Args... args) : cc(cc), env(env) {
      /*
      for (unsigned i = 0; i < (1 << PARTITION_BITS); ++i) {
	op_target_partitions.emplace_back(
	  cc, args...);
      }
      */
      env->register_message_listener(this);
    }

    ~Client() {
      env->unregister_message_listener(this);
    }

    ClientPartition<P> &get_partition(uint32_t hash) {
      return op_target_partitions[
	hash & ((1 << PARTITION_BITS) - 1)];
    }
    
    Ceph::Future<typename CommandController::Response> submit_command(
      Command c) {
      auto hoid = cc.get_metadata(c).get_target();
      return get_partition(hoid.get_hash()).submit_command(
	std::move(c));
    }

    bool deliver_message(MessageRef m) {
      if (m->get_type() == MSG_OSD_PPG_RESPONSE) {
	boost::intrusive_ptr<MSPPGResponse> response =
	  static_cast<MSPPGResponse*>(
	    m.detach());
	auto &partition = get_partition(response->get_hash());
	Ceph::Future<> to_schedule;
	env->submit_task(
	  &partition,
	  to_schedule.then_ignore_error(
	    [&partition, response{std::move(response)}](){
	      partition.deliver_message(std::move(response));
	    }));
	return true;
      } else {
	return false;
      }
    }

    void connection_reset(Connection *con) {
      for (auto &partition: op_target_partitions) {
	Ceph::Future<> to_schedule;
	env->submit_task(
	  &partition,
	  Ceph::Future<>::make_ready_value().then_ignore_error(
	    [&partition, con](){
	      partition.connection_reset(con);
	    }));
      }
    }
  };

public:
  MultiPaxosProtocol(config_t conf)
    : conf(conf) {}

  PaxosPG::Consensus::ClientRef<P> get_client(
    CommandController cc,
    Environment::Ref env,
    ClusterMap::Ref curmap
    ) {
    return std::make_unique<Client>(
      std::move(cc),
      std::move(env),
      conf,
      std::move(curmap));
  }

  PaxosPG::Consensus::ServerRef<P> get_server(
    typename Processor::Ref processor,
    ObjectStore *obj,
    PaxosPG::Environment::Ref env
    ) {
    return std::make_unique<Server<P>>(conf, std::move(env));
  }
};

};
};
};

#endif
