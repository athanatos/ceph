// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "dmclock/src/dmclock_recs.h"

// the following is done to unclobber _ASSERT_H so it returns to the
// way ceph likes it
#include "include/ceph_assert.h"

#include "include/denc.h"


namespace ceph {
namespace qos {

struct mclock_profile_params_t {
  uint64_t reservation;
  uint64_t weight;
  uint64_t limit;

  // unique for a given client, so a <client_id, qos_profile_id> tuple
  // is a unique identifier
  uint64_t profile_id;

  mclock_profile_params_t() :
    reservation(0), weight(0), limit(0), profile_id(0)
  {}

  mclock_profile_params_t(uint64_t r, uint64_t w, uint64_t l, uint64_t qpid) :
    reservation(r),
    weight(w),
    limit(l),
    profile_id(qpid)
  {}

  DENC(mclock_profile_params_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.reservation, p);
    denc(v.limit, p);
    denc(v.weight, p);
    denc(v.profile_id, p);
    DENC_FINISH(p);
  }
};

struct dmclock_request_t {
  crimson::dmclock::ReqParams r;

  dmclock_request_t() = default;
  dmclock_request_t(crimson::dmclock::ReqParams r) : r(r) {}

  DENC(dmclock_request_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.r.delta, p);
    denc(v.r.rho, p);
    DENC_FINISH(p);
  }
};

struct dmclock_response_t {
  crimson::dmclock::PhaseType phase = crimson::dmclock::PhaseType::reservation;
  uint64_t cost = 0;

  DENC(dmclock_response_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.phase, p);
    denc(v.cost, p);
    DENC_FINISH(p);
  }
};

}
}
WRITE_CLASS_DENC(ceph::qos::mclock_profile_params_t)
WRITE_CLASS_DENC(ceph::qos::dmclock_request_t);
WRITE_CLASS_DENC(ceph::qos::dmclock_response_t);


