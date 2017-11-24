// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_PAXOSPG_MOCK_CONSENSUS_H
#define CEPH_PAXOSPG_MOCK_CONSENSUS_H

#include <memory>
#include "common/hobject.h"
#include "paxospg/Consensus.h"
#include "paxospg/types.h"

class IncrementMetadata : public PaxosPG::Consensus::CommandMetadataI {
  hobject_t hoid;
public:
  IncrementMetadata() = default;
  IncrementMetadata(hobject_t hoid) : hoid(hoid) {}

  hobject_t get_target() const {
    return hobject_t();
  }

  DENC(IncrementMetadata, v, p) {
    DENC_START(1, 1, p);
    ::denc(v.hoid, p);
    DENC_FINISH(p);
  }
  ~IncrementMetadata() = default;
};
WRITE_CLASS_DENC(IncrementMetadata)

class Increment {
  IncrementMetadata metadata;
public:
  Increment() = default;
  Increment(const Increment&) = delete;
  Increment(Increment &&) = default;
  Increment(hobject_t hoid) : metadata(hoid) {}

  const IncrementMetadata &get_metadata() const { return metadata; }
  Increment &operator=(const Increment&) = delete;
  Increment &operator=(Increment&&) = default;

  DENC(Increment, v, p) {
    DENC_START(1, 1, p);
    ::denc(v.metadata, p);
    DENC_FINISH(p);
  }
  Increment clone() const {
    return Increment(get_metadata().get_target());
  }
  ~Increment() = default;
};
WRITE_CLASS_DENC(Increment)

class Count {
  uint64_t val;
public:
  Count() : val(0) {}
  Count(uint64_t val) : val(val) {}

  DENC(Count, v, p) {
    DENC_START(1, 1, p);
    ::denc(v.val, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(Count)

class IncrementCommandController :
  public PaxosPG::Consensus::CommandControllerI<
    Increment,
    IncrementMetadata,
    Increment,
    Count,
    Count> {
public:
  const IncrementMetadata &get_metadata(
    const Increment &c) const {
    return c.get_metadata();
  }

  void get_ranked_encoding(
    const Command &c,
    size_t rank_len,
    const bool *to_generate, // null for all
    CommandRankedEncoding *out) const {
    for (uint8_t i = 0; i < rank_len; ++i) {
      if (to_generate[i])
	out[i] = c.clone();
    }
  }

  boost::optional<std::set<pg_shard_t>> min_to_recover(
    const std::set<pg_shard_t> &in) const {
    if (in.empty()) {
      return boost::none;
    } else {
      return std::set<pg_shard_t>(in.begin(), ++in.begin());
    }
  }
  Increment from_command_ranked_encoding(
    const std::map<pg_shard_t, Increment> &in) const {
    return in.begin()->second.clone();
  }
  Count from_response_ranked_encoding(
    const std::map<pg_shard_t, Count> &in) const {
    return in.begin()->second;
  }
};

class Counter :
  public PaxosPG::Consensus::ProcessorI<IncrementCommandController> {
  ObjectStore *store;

public:
  Counter(ObjectStore *store)
    : store(store) {}

  Ceph::Future<Count> process_command(
    const Increment &c,
    ObjectStore::Transaction *t);
};

template <typename P>
class MockProtocol : public PaxosPG::Consensus::Protocol<P> {

  class Client : public PaxosPG::Consensus::Client<P> {
  public:
    Client() = default;

    Ceph::Future<typename P::Response> submit_command(
      typename P::Command c) {
      return Ceph::Future<typename P::Response>::make_ready_value();
    }
  };

  class Server : public PaxosPG::Consensus::Server<P> {
  public:
    Server() = default;
  };

public:
  MockProtocol() = default;

  PaxosPG::Consensus::ClientRef<P> get_client(
    typename P::CommandController command_controller,
    PaxosPG::Environment::Ref env,
    PaxosPG::ClusterMap::Ref curmap
    ) {
    return std::make_unique<Client>();
  }

  PaxosPG::Consensus::ServerRef<P> get_server(
    typename P::Ref processor,
    ObjectStore *obj,
    PaxosPG::Environment::Ref env
    ) {
    return std::make_unique<Server>();
  }
};

#endif
