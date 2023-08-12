// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "primary_scrub_process.h"
#include "crimson/osd/pg.h"

namespace crimson::osd {

PrimaryScrubProcess::PrimaryScrubProcess(Ref<PG> pg) : pg(pg)
{
  pg->primary_scrub_process = this;
}

PrimaryScrubProcess::~PrimaryScrubProcess()
{
  pg->primary_scrub_process = nullptr;
}

seastar::future<> PrimaryScrubProcess::start()
{
  return seastar::now();
}

void PrimaryScrubProcess::print(std::ostream &) const
{
}

void PrimaryScrubProcess::dump_detail(Formatter *f) const
{
}

}
