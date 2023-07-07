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

void PrimaryActive::exit()
{
  if (local_reservation_held) {
    get_scrub_context().cancel_local_reservation();
  }
  if (remote_reservations_held) {
    // TODO: guard from interval change
    get_scrub_context().cancel_remote_reservations();
  }
}

GetLocalReservation::GetLocalReservation(my_context ctx) : ScrubState(ctx)
{
  context<PrimaryActive>().local_reservation_held = true;
  get_scrub_context().request_local_reservation();
}

GetRemoteReservations::GetRemoteReservations(my_context ctx) : ScrubState(ctx)
{
  context<PrimaryActive>().remote_reservations_held = true;
  get_scrub_context().request_remote_reservations();
}

Scrubbing::Scrubbing(my_context ctx) : ScrubState(ctx)
{
}

};
