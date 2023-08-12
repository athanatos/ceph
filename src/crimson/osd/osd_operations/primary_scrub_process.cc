// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "primary_scrub_process.h"

namespace crimson::osd {

// imposed by `ShardService::start_operation<T>(...)`.
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
