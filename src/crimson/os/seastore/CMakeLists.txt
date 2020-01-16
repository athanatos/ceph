add_library(crimson-seastore
  segment_manager/ephemeral.cc 
  segment_manager.cc
  transaction_manager.cc
  seastore.cc)
target_link_libraries(crimson-seastore
  crimson)
