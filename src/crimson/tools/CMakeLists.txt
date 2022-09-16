list(APPEND crimson-store-nbd-srcs
  store_nbd/store-nbd.cc
  store_nbd/tm_driver.cc
  store_nbd/fs_driver.cc
  store_nbd/block_driver.cc
  store_nbd/iofrontend.cc
  store_nbd/nbd_frontend.cc
)

if(WITH_SPDK)
  list(APPEND crimson-store-nbd-srcs
    store_nbd/nvmeof_frontend.cc
  )
endif()

add_executable(crimson-store-nbd ${crimson-store-nbd-srcs})
target_link_libraries(crimson-store-nbd
  PUBLIC crimson-os)

if(WITH_SPDK)
  target_link_libraries(crimson-store-nbd
    PRIVATE spdk::spdk)
endif()

install(TARGETS crimson-store-nbd DESTINATION bin)
