// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"

#include "crimson/osd/scrub/scrub_machine.h"

namespace sc = boost::statechart;

namespace crimson::osd::scrub {

Crash::Crash(my_context ctx) : ScrubState(ctx)
{
  ceph_abort("Crash state impossible");
}

};
