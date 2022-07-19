// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <limits>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"

namespace crimson::osd {
class PG;

class PGMap {
  using mapping_id_t = unsigned;
  static constexpr mapping_id_t NULL_MAP =
    std::numeric_limits<mapping_id_t>::max();

  struct PGPlacement : BlockerT<PGPlacement> {
    static constexpr const char * type_name = "PGPlacement";
    const spg_t pgid;

    /* While is PG is being created (or loaded), contains a promise that
     * will become available once the PG is ready.  Otherwise, nullopt. */
    std::optional<seastar::shared_promise<>> on_available;

    // null until created, only accessible on specified core
    Ref<PG> pg;

    PGPlacement(spg_t pgid);
    ~PGPlacement();

    void set_created(Ref<PG> _pg);

    PGPlacement(const PGPlacement &) = delete;
    PGPlacement(PGPlacement &&) = delete;
    PGPlacement &operator=(const PGPlacement &) = delete;
    PGPlacement &operator=(PGPlacement &&) = delete;

    void dump_detail(Formatter *f) const final;
  };

  std::map<spg_t, PGPlacement> pgs;

public:
  using PGCreationBlocker = PGPlacement;
  using PGCreationBlockingEvent = PGCreationBlocker::BlockingEvent;
  /**
   * Get future for pg with a bool indicating whether it's already being
   * created.
   */
  std::pair<seastar::future<Ref<PG>>, bool>
  wait_for_pg(PGCreationBlockingEvent::TriggerI&&, spg_t pgid);

  /**
   * get PG in non-blocking manner
   */
  Ref<PG> get_pg(spg_t pgid);

  /**
   * Set creating
   */
  void set_creating(spg_t pgid);

  /**
   * Set newly created pg
   */
  void pg_created(spg_t pgid, Ref<PG> pg);

  auto get_num_pgs() const { return pgs.size(); }

  PGMap() = default;
  ~PGMap();
};

}
