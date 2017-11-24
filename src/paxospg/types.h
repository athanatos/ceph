// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_PAXOSPG_TYPES_H
#define CEPH_PAXOSPG_TYPES_H

#include <memory>
#include <chrono>

#include "include/cmp.h"
#include "include/types.h"
#include "msg/Message.h"
#include "msg/Messenger.h"
#include "osd/osd_types.h"
#include "common/Future.h"

namespace PaxosPG {

/**
 * Interface for quorum stream
 */
class ClusterMap {
public:
  using Ref = std::shared_ptr<const ClusterMap>;

  class Listener {
  public:
    virtual void next_quorum(
      Ref map
      ) = 0;
    virtual ~Listener() {}
  };

  class Source {
  public:
    virtual void get_maps_from(
      epoch_t from,
      Listener *l) = 0;
    virtual void get_maps(
      Listener *l) = 0;
    virtual void unregister_listener(
      Listener *l) = 0;
    virtual ~Source() {}
  };

  virtual epoch_t get_epoch() const = 0;
  virtual void get_mapping(
    pg_t pg,
    osd_id_t mapping[],
    size_t mapping_size) const = 0;
  virtual entity_inst_t get_osd_address(osd_id_t osd) const = 0;
  virtual pg_t get_pg(
    const hobject_t &hoid) const = 0;
    
  virtual ~ClusterMap() {}
};

/**
 * Environment provided to Protocol implementations
 */
class Environment {
public:
  using Ref = std::shared_ptr<Environment>;

  // Time -- must be monotonic
  using time_point = std::chrono::steady_clock::time_point;
  using duration = std::chrono::duration<double>;
  virtual time_point now() {
    return std::chrono::steady_clock::now();
  }

  // Tids
  virtual ceph_tid_t get_tid() {
    return 0; // todo
  }

  // Task scheduling
  virtual void submit_task(
    uint64_t ordering_token,
    duration schedule_after,
    Ceph::Future<> &&f) = 0;
  template <typename T>
  void submit_task(T ordering_token, Ceph::Future<> &&f) {
    submit_task(std::hash<T>{}(ordering_token), duration::zero(), std::move(f));
  }
  template <typename T>
  void submit_task(
    T ordering_token, duration schedule_after, Ceph::Future<> &&f) {
    submit_task(std::hash<T>{}(ordering_token), schedule_after, std::move(f));
  }

  // Messages
  virtual entity_inst_t get_inst(osd_id_t osd) const = 0;
  virtual ConnectionRef get_connection(const entity_inst_t& dest) = 0;
  virtual ConnectionRef get_loopback_connection() = 0;

  class MessageListener {
  public:
    virtual bool deliver_message(MessageRef m) = 0;

    // Not granted reference
    virtual void connection_reset(Connection *con) = 0;
    virtual ~MessageListener() = default;
  };
  virtual void register_message_listener(MessageListener *l) = 0;
  virtual void unregister_message_listener(MessageListener *l) = 0;

  virtual ClusterMap::Source &get_cluster_map_source() = 0;
  virtual ~Environment() = default;
};

}; // namespace

#endif
