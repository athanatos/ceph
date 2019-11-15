// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <vector>
#include <string>

#include "osd/OSDMap.h"

class MOSDOp;
class OpInfo {
public:
  struct ClassInfo {
    ClassInfo(std::string&& class_name, std::string&& method_name,
              bool read, bool write, bool whitelisted) :
      class_name(std::move(class_name)), method_name(std::move(method_name)),
      read(read), write(write), whitelisted(whitelisted)
    {}
    const std::string class_name;
    const std::string method_name;
    const bool read, write, whitelisted;
  };

private:
  uint64_t rmw_flags = 0;
  std::vector<ClassInfo> classes;

  void set_rmw_flags(int flags);

  void add_class(std::string&& class_name, std::string&& method_name,
                 bool read, bool write, bool whitelisted) {
    classes.emplace_back(std::move(class_name), std::move(method_name),
                          read, write, whitelisted);
  }

public:

  void clear() {
    rmw_flags = 0;
  }

  uint64_t get_flags() const {
    return rmw_flags;
  }

  bool check_rmw(int flag) const ;
  bool may_read() const;
  bool may_write() const;
  bool may_cache() const;
  bool rwordered_forced() const;
  bool rwordered() const;
  bool includes_pg_op() const;
  bool need_read_cap() const;
  bool need_write_cap() const;
  bool need_promote() const;
  bool need_skip_handle_cache() const;
  bool need_skip_promote() const;
  bool allows_returnvec() const;

  void set_read();
  void set_write();
  void set_cache();
  void set_class_read();
  void set_class_write();
  void set_pg_op();
  void set_promote();
  void set_skip_handle_cache();
  void set_skip_promote();
  void set_force_rwordered();
  void set_returnvec();

  int set_from_op(
    const MOSDOp *m,
    const OSDMap &osdmap,
    bool assume_simple=false);

  std::vector<ClassInfo> get_classes() const {
    return classes;
  }
};

std::ostream& operator<<(std::ostream& out, const OpInfo::ClassInfo& i);
