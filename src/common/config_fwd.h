// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#if defined (WITH_SEASTAR) && !defined (WITH_ALIEN)
namespace ceph::common {
  class ConfigProxy;
}
using ConfigProxy = ceph::common::ConfigProxy;
#else
class ConfigProxy;
#endif
