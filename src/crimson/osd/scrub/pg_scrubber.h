// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "scrub_machine.h"

namespace crimson::osd {
class PG;
}

namespace crimson::osd::scrub {

class PGScrubber : public ScrubContext {
  PG &pg;

public:
  PGScrubber(PG &pg) : pg(pg) {}

  void scrub_requested();

private:
  // ScrubContext interface
  pg_shard_t get_my_id() const final {
    return pg_shard_t{};
  }

  std::set<pg_shard_t> ids_to_scrub;
  const std::set<pg_shard_t> &get_ids_to_scrub() const final {
    return ids_to_scrub;
  }

  chunk_validation_policy_t policy;
  const chunk_validation_policy_t &get_policy() const final {
    return policy;
  }

  void request_local_reservation() final {
  }

  void cancel_local_reservation() final {
  }

  void replica_request_local_reservation() final {
  }

  void replica_cancel_local_reservation() final {
  }

  void replica_confirm_reservation() final {
  }

  void request_remote_reservation(pg_shard_t target) final {
  }

  void cancel_remote_reservation(pg_shard_t target) final {
  }

  void request_range(const hobject_t &start) final {
  }

  void reserve_range(const hobject_t &start, const hobject_t &end) final {
  }

  void release_range() final {
  }

  void scan_range(
    pg_shard_t target,
    const hobject_t &start,
    const hobject_t &end) final {
  }

  void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) final {
  }
};
  
};
