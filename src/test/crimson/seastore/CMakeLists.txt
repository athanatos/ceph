add_executable(unittest_transaction_manager
  test_transaction_manager.cc
  $<TARGET_OBJECTS:unit-main>)
add_ceph_test(unittest_transaction_manager
  unittest_transaction_manager --memory 256M --smp 1)
target_link_libraries(unittest_transaction_manager crimson-seastore)
