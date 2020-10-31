add_executable(crimson-store-nbd
	store-nbd.cc
  )
target_link_libraries(crimson-store-nbd
  crimson-os
  crimson-common
  crimson-seastore)
install(TARGETS crimson-store-nbd DESTINATION bin)
