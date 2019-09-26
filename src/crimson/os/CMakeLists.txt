include_directories(SYSTEM "${CMAKE_SOURCE_DIR}/src/rocksdb/include")
set(crimson_os_srcs
  futurized_store.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc)

if(NOT WITH_BLUESTORE)
  list(APPEND crimson_os_srcs
  cyan_collection.cc
  cyan_object.cc
  cyan_store.cc)
  add_library(crimson-os STATIC ${crimson_os_srcs})
endif(NOT WITH_BLUESTORE)

if(WITH_BLUESTORE)
  list(APPEND crimson_os_srcs
    alien_store.cc
    ${PROJECT_SOURCE_DIR}/src/common/admin_socket.cc
    ${PROJECT_SOURCE_DIR}/src/common/blkdev.cc
    ${PROJECT_SOURCE_DIR}/src/common/ceph_context.cc
    ${PROJECT_SOURCE_DIR}/src/common/ceph_crypto.cc
    ${PROJECT_SOURCE_DIR}/src/common/condition_variable_debug.cc
    ${PROJECT_SOURCE_DIR}/src/common/cmdparse.cc
    ${PROJECT_SOURCE_DIR}/src/common/Finisher.cc
    ${PROJECT_SOURCE_DIR}/src/common/HeartbeatMap.cc
    ${PROJECT_SOURCE_DIR}/src/common/PluginRegistry.cc
    ${PROJECT_SOURCE_DIR}/src/common/lockdep.cc
    ${PROJECT_SOURCE_DIR}/src/common/perf_counters.cc
    ${PROJECT_SOURCE_DIR}/src/common/perf_counters_collection.cc
    ${PROJECT_SOURCE_DIR}/src/common/RefCountedObj.cc
    ${PROJECT_SOURCE_DIR}/src/common/shared_mutex_debug.cc
    ${PROJECT_SOURCE_DIR}/src/common/Throttle.cc
    ${PROJECT_SOURCE_DIR}/src/common/Timer.cc
    ${PROJECT_SOURCE_DIR}/src/common/TrackedOp.cc
    ${PROJECT_SOURCE_DIR}/src/common/WorkQueue.cc
    ${PROJECT_SOURCE_DIR}/src/common/util.cc
    ${PROJECT_SOURCE_DIR}/src/global/global_context.cc
    ${PROJECT_SOURCE_DIR}/src/os/ObjectStore.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/Allocator.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapFreelistManager.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlockDevice.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueFS.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluefs_types.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueRocksEnv.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueStore.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/bluestore_types.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/fastbmap_allocator_impl.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/FreelistManager.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/StupidAllocator.cc
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BitmapAllocator.cc)
endif(WITH_BLUESTORE)

if(HAVE_LIBAIO OR HAVE_POSIXAIO)
  list(APPEND crimson_os_srcs
     ${PROJECT_SOURCE_DIR}/src/os/bluestore/KernelDevice.cc
     ${PROJECT_SOURCE_DIR}/src/os/bluestore/aio.cc)
endif()

if(WITH_BLUEFS)
  add_library(bluefs SHARED
    ${PROJECT_SOURCE_DIR}/src/os/bluestore/BlueRocksEnv.cc)
  target_include_directories(bluefs SYSTEM PUBLIC
    $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(bluefs global)
  install(TARGETS bluefs DESTINATION lib)
endif(WITH_BLUEFS)

if(WITH_BLUESTORE)
  add_library(crimson-os STATIC ${crimson_os_srcs}
    $<TARGET_OBJECTS:compressor_objs>
    $<TARGET_OBJECTS:crush_objs>
    $<TARGET_OBJECTS:common_prioritycache_obj>)
if(HAVE_LIBAIO)
  target_link_libraries(crimson-os ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

  target_compile_definitions(crimson-os PRIVATE -DWITH_SEASTAR -DWITH_ALIEN)
  target_link_libraries(crimson-os kv)
  target_link_libraries(crimson-os heap_profiler)
  target_link_libraries(crimson-os ${BLKID_LIBRARIES})
  target_link_libraries(crimson-os ${UDEV_LIBRARIES})
endif(WITH_BLUESTORE)

target_link_libraries(crimson-os crimson)
