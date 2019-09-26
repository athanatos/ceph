// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_LOCATION_H
#define CEPH_CRUSH_LOCATION_H

#include <iosfwd>
#include <map>
#include <string>

#include "common/ceph_mutex.h"
#include "crimson/os/with_alien.h"

#if defined (WITH_SEASTAR) && !defined (WITH_ALIEN)
namespace ceph::common {
#endif

class CrushLocation {
public:
  explicit CrushLocation(CephContext *c) : cct(c) {
    init_on_startup();
  }

  int update_from_conf();  ///< refresh from config
  int update_from_hook();  ///< call hook, if present
  int init_on_startup();

  std::multimap<std::string,std::string> get_location() const;

private:
  int _parse(const std::string& s);
  CephContext *cct;
  std::multimap<std::string,std::string> loc;
  mutable ceph::mutex lock = ceph::make_mutex("CrushLocation");
};

std::ostream& operator<<(std::ostream& os, const CrushLocation& loc);
#if defined (WITH_SEASTAR) && !defined (WITH_ALIEN)
}
#endif
#endif
