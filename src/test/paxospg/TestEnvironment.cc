// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestEnvironment.h"

using namespace PaxosPG;

void TestCoordinator::TestEnvironment::submit_task(
  uint64_t ordering_token, duration schedule_after, Ceph::Future<> &&f)
{
}

// Messages
entity_inst_t TestCoordinator::TestEnvironment::get_inst(osd_id_t osd) const
{
  return entity_inst_t();
}

ConnectionRef TestCoordinator::TestEnvironment::get_connection(
  const entity_inst_t& dest)
{
  return ConnectionRef();
}

ConnectionRef TestCoordinator::TestEnvironment::get_loopback_connection()
{
  return ConnectionRef();
}

void TestCoordinator::TestEnvironment::register_message_listener(
  PaxosPG::Environment::MessageListener *l) {
}

void TestCoordinator::TestEnvironment::unregister_message_listener(
  PaxosPG::Environment::MessageListener *l) {
}

PaxosPG::ClusterMap::Source &
TestCoordinator::TestEnvironment::get_cluster_map_source()
{
  return *(PaxosPG::ClusterMap::Source*)nullptr;
}

Environment::Ref TestCoordinator::get_environment()
{
  return Environment::Ref(new TestEnvironment);
}
