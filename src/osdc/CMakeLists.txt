set(osdc_files
  Filer.cc
  ObjectCacher.cc
  Objecter.cc
  Striper.cc
  QosProfileMgr.cc)
add_library(osdc STATIC ${osdc_files})
if(WITH_LTTNG AND WITH_EVENTTRACE)
  add_dependencies(osdc eventtrace_tp)
  add_definitions("-DWITH_EVENTTRACE")
endif()
target_link_libraries(osdc dmclock)
