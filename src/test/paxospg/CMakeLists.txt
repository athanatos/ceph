# unittest_bloom_filter
add_executable(unittest_test_consensus
  TestConsensus.cc
	TestEnvironment.cc
	ConsensusTestMocks.cc
  )
add_ceph_unittest(unittest_test_consensus)
target_link_libraries(unittest_test_consensus ceph-common)
