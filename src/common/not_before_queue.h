// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/utime.h"

/**
 * not_before_queue_t
 *
 * Implements a generic queue on queued value type V with the following
 * free functions defined:
 *  - utime_t project_not_before(const V&)
 *  - bool operator<(const lhs&, const rhs&)
 *
 * V must also have a copy constructor.
 *
 * The purpose of this queue implementation is to add a not_before concept
 * to allow specifying a point in time before which the item will not be
 * eligible for dequeueing.  Once that point is passed, ordering is determined
 * by the operator< definition.
 */
template <typename V>
class not_before_queue_t {
  struct container_t: public boost::intrusive_ref_counter<
    container_t, boost::thread_safe_counter
    > {
    const V v;
    using ref_t = boost::intrusive_ptr<container_t>;
  };
public:
};
