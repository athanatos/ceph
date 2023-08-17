// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/pg.h"
#include "pg_scrubber.h"

namespace crimson::osd::scrub {

void PGScrubber::on_primary_activate()
{
}

void PGScrubber::on_interval_change()
{
}

void PGScrubber::handle_scrub_requested()
{
  if (pg.peering_state.is_active()) {
    machine.process_event(StartScrub{});
  }
}

void PGScrubber::handle_scrub_message(Message &m)
{
}

}
