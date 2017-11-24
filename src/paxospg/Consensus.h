// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_PAXOSPG_CONSENSUS_H
#define CEPH_PAXOSPG_CONSENSUS_H

#include <memory>
#include <map>
#include <boost/optional.hpp>

#include "common/Future.h"
#include "common/hobject.h"
#include "include/buffer.h"
#include "include/types.h"
#include "types.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"

namespace PaxosPG {
namespace Consensus {

class CommandMetadataI {
public:
  virtual hobject_t get_target() const = 0;
  virtual ~CommandMetadataI() = default;
};

/**
 * Command controller
 */
template <typename C, typename CM, typename CRE, typename R, typename RRE>
class CommandControllerI {
public:
  using Command = C;
  using CommandMetadata = CM;
  using CommandRankedEncoding = CRE;
  using Response = R;
  using ResponseRankedEncoding = RRE;
  virtual const CommandMetadata &get_metadata(const Command &) const = 0;
  virtual void get_ranked_encoding(
    const Command &c,
    size_t rank_len,
    const bool *to_generate, // null for all
    CommandRankedEncoding *out) const = 0;

  virtual boost::optional<std::set<pg_shard_t>> min_to_recover(
    const std::set<pg_shard_t> &in) const = 0;
  virtual Command from_command_ranked_encoding(
    const std::map<pg_shard_t, CommandRankedEncoding> &) const = 0;
  virtual Response from_response_ranked_encoding(
    const std::map<pg_shard_t, ResponseRankedEncoding> &) const = 0;
  virtual ~CommandControllerI() = default;
};

template <typename CC>
struct validate_command_controller_type {
  static_assert(
    denc_traits<typename CC::Command>::supported,
    "CC::Command must support denc");
  static_assert(
    denc_traits<typename CC::CommandMetadata>::supported,
    "CC::CommandMetadata must support denc");
  static_assert(
    denc_traits<typename CC::CommandRankedEncoding>::supported,
    "CC::CommandRankedEncoding must support denc");
  static_assert(
    denc_traits<typename CC::Response>::supported,
    "CC::Response must support denc");
  static_assert(
    denc_traits<typename CC::ResponseRankedEncoding>::supported,
    "CC::ResponseRankedEncoding must support denc");

  static_assert(
    std::is_copy_constructible<typename CC::CommandMetadata>::value,
    "Metadata must be copyable");

  static_assert(
    std::is_base_of<CommandMetadataI, typename CC::CommandMetadata>::value,
    "CC::CommandMetadata must inherit from CommandMetadataI");

  static_assert(
    std::is_base_of<
      CommandControllerI<
        typename CC::Command,
        typename CC::CommandMetadata,
        typename CC::CommandRankedEncoding,
        typename CC::Response,
        typename CC::ResponseRankedEncoding
        >, CC>::value,
    "Must have P::CommandController implementing CommandControllerI");
};

/**
 * Interface required for any Processor
 */
template <typename CC>
class ProcessorI {
  validate_command_controller_type<CC> check;
public:
  using Command = typename CC::Command;
  using Response = typename CC::Response;
  using CommandController = CC;
  using Ref = std::unique_ptr<ProcessorI<CC>>;
  virtual Ceph::Future<Response> process_command(
    const Command &c,
    ObjectStore::Transaction *t) = 0;
  virtual const CommandController &get_command_controller() const = 0;
  virtual ~ProcessorI() = default;
};


template <typename P>
struct validate_processor_type {
  validate_command_controller_type<typename P::CommandController> check;

  static_assert(
    std::is_base_of<
      ProcessorI<typename P::CommandController>,
      P>::value,
    "Must inherit from Processor");
};

/**
 * Interface for Consensus clients
 */
template <typename P>
class Client {
  validate_processor_type<P> check;

public:
  virtual Ceph::Future<typename P::Response> submit_command(
    typename P::Command c) = 0;

  virtual ~Client() = default;
};
template <typename P>
using ClientRef = std::unique_ptr<Client<P>>;

/**
 * Interface for Consensus acceptors
 */
template <typename P>
class Server {
  validate_processor_type<P> check;
public:
  virtual ~Server() = default;
};
template<typename P>
using ServerRef = std::unique_ptr<Server<P>>;

template <typename P>
class Protocol {
  validate_processor_type<P> check;

public:
  virtual ClientRef<P> get_client(
    typename P::CommandController command_controller,
    Environment::Ref env,
    ClusterMap::Ref curmap
    ) = 0;
  virtual ServerRef<P> get_server(
    typename P::Ref processor,
    ObjectStore *obj,
    Environment::Ref env
    ) = 0;
  virtual ~Protocol() = default;
};

}; // namespace Consensus
}; // namespace PaxosPG

#endif
