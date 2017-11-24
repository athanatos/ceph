// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_PAXOSPG_TEST_ENVIRONMENT_H
#define CEPH_PAXOSPG_TEST_ENVIRONMENT_H

#include "paxospg/types.h"

class TestCoordinator {

  class TestEnvironment : public PaxosPG::Environment {
  public:
    // Task scheduling
    void submit_task(
      uint64_t ordering_token,
      duration schedle_after,
      Ceph::Future<> &&f) override;

    // Messages
    entity_inst_t get_inst(osd_id_t osd) const override;
    ConnectionRef get_connection(const entity_inst_t& dest) override;
    ConnectionRef get_loopback_connection() override;
    void register_message_listener(MessageListener *l);
    void unregister_message_listener(MessageListener *l);

    PaxosPG::ClusterMap::Source &get_cluster_map_source() override;
  };
public:
  PaxosPG::Environment::Ref get_environment();
};

#endif
