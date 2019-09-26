#include "futurized_store.h"
#ifdef WITH_BLUESTORE
#include "alien_store.h"
#else
#include "cyan_store.h"
#endif

namespace ceph::os {

std::unique_ptr<FuturizedStore> FuturizedStore::create(const std::string& type,
                                       const std::string& data, ConfigValues* values)
{
#ifndef WITH_BLUESTORE
  if (type == "memstore") {
    return std::make_unique<ceph::os::CyanStore>(data);
  }
#else
  if (type == "bluestore") {
    return std::make_unique<ceph::os::AlienStore>(data, values);
  }
#endif

  ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
  return {};
}

}
