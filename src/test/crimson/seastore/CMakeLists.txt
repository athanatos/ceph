add_executable(unittest_transaction_manager
  test_transaction_manager.cc
  test/crimson/gtest_seastar.cc)
add_ceph_unittest(unittest_transaction_manager)
target_link_libraries(
  unittest_transaction_manager
  ${CMAKE_DL_LIBS} 
  crimson-seastore)
