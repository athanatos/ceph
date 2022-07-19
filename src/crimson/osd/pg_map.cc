// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_map.h"

#include "crimson/osd/pg.h"
#include "common/Formatter.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void PGMap::PGPlacement::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_bool("creating", on_available.has_value());
}

void PGMap::PGPlacement::set_created(Ref<PG> _pg, mapping_id_t _core_mapping)
{
  assert(!pg);
  pg = _pg;

  assert(core_mapping == NULL_MAP);
  core_mapping = _core_mapping;

  assert(on_available);
  on_available->set_value();
  on_available = std::nullopt;
}

PGMap::PGPlacement::PGPlacement(spg_t pgid)
  : pgid(pgid), on_available(seastar::shared_promise<>()) {}
PGMap::PGPlacement::~PGPlacement() {}

std::pair<seastar::future<Ref<PG>>, bool>
PGMap::wait_for_pg(PGCreationBlockingEvent::TriggerI&& trigger, spg_t pgid)
{
  auto pgiter = pgs.find(pgid);
  if (pgiter == pgs.end()) {
    pgiter = pgs.emplace(pgid, pgid).first;
  }

  if (pgiter->second.pg) {
    return std::make_pair(
      seastar::make_ready_future<Ref<PG>>(pgiter->second.pg),
      true);
  }

  auto &mapping = pgiter->second;
  assert(mapping.on_available);
  auto retfut = mapping.on_available->get_shared_future(
  ).then([&mapping] {
    return mapping.pg;
  });
  return std::make_pair(
    // TODO: add to blocker Trigger-taking make_blocking_future
    trigger.maybe_record_blocking(std::move(retfut), mapping),
    false);
}

Ref<PG> PGMap::get_pg(spg_t pgid)
{
  if (auto pg = pgs.find(pgid); pg != pgs.end() && pg->second.pg) {
    return pg->second.pg;
  } else {
    return nullptr;
  }
}

void PGMap::set_creating(spg_t pgid)
{
  logger().debug("Creating {}", pgid);
  ceph_assert(pgs.count(pgid) == 0);
  pgs.emplace(pgid, pgid);
}

void PGMap::pg_created(spg_t pgid, Ref<PG> pg)
{
  logger().debug("Created {}", pgid);
  auto pgiter = pgs.find(pgid);
  ceph_assert(pgiter != pgs.end());
  pgiter->second.set_created(pg, 0 /* TODOSAM */);
}

PGMap::~PGMap() {}

}
