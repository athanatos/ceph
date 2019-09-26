#pragma once

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
namespace ceph::common {
  class CephContext;
}
using CephContext = ceph::common::CephContext;
#else
class CephContext;
#endif
