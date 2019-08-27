// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>
#include <mutex>

#include "QosProfileMgr.h"


osdc::qos_profile_ref osdc::QosProfileMgr::create(uint64_t r,
						  uint64_t w,
						  uint64_t l)
{
  return qos_profile_ref(
    new QosProfile(r, w, l, next_client_profile_id++)
  );
}
