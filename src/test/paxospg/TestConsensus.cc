// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include "paxospg/Consensus.h"
#include "paxospg/protocol/multi/MultiPaxosProtocol.h"
#include "TestEnvironment.h"
#include "ConsensusTestMocks.h"
#include "common/hobject.h"
#include "include/denc.h"

#include "gtest/gtest.h"

using namespace PaxosPG::Consensus;

struct ConsensusTest : public ::testing::Test {
  TestCoordinator environment;
  virtual ~ConsensusTest() = default;
};

TEST_F(ConsensusTest, interfaces)
{
  MockProtocol<Counter> c;
  ClientRef<Counter> client = c.get_client(
    IncrementCommandController(),
    environment.get_environment(),
    PaxosPG::ClusterMap::Ref());
}

TEST_F(ConsensusTest, multipaxos)
{
  using namespace PaxosPG::Protocol::MultiPaxos;
  MultiPaxosProtocol<Counter> c(config_t(10, 6, 7));
  ClientRef<Counter> client = c.get_client(
    IncrementCommandController(),
    environment.get_environment(),
    PaxosPG::ClusterMap::Ref());
}
