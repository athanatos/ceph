// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replica_scrub_process.h"
#include "crimson/osd/pg.h"

namespace crimson::osd {

ReplicaScrubProcess::ReplicaScrubProcess(Ref<PG> pg) : pg(pg)
{
  pg->replica_scrub_process = this;
}

ReplicaScrubProcess::~ReplicaScrubProcess()
{
  pg->replica_scrub_process = nullptr;
}


seastar::future<> ReplicaScrubProcess::start()
{
  return seastar::now();
}

void ReplicaScrubProcess::print(std::ostream &) const
{
}

void ReplicaScrubProcess::dump_detail(Formatter *f) const
{
}

}
