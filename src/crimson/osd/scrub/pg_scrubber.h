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
  ScrubMachine machine;

public:
  static inline bool is_scrub_message(Message &m) {
    switch (m.get_type()) {
    case MSG_OSD_REP_SCRUB:
      return true;
    default:
      return false;
    }
    return false;
  }

  PGScrubber(PG &pg) : pg(pg), machine(*this) {}

  void on_primary_activate();
  void on_replica_activate();
  void on_interval_change();

  void handle_scrub_requested();

  void handle_scrub_message(Message &m);

private:
  // ScrubContext interface
  pg_shard_t get_my_id() const final {
    return pg_shard_t{};
  }

  const std::set<pg_shard_t> &get_ids_to_scrub() const final;

  chunk_validation_policy_t policy;
  const chunk_validation_policy_t &get_policy() const final {
    return policy;
  }

  void request_range(const hobject_t &start) final;
  eversion_t reserve_range(const hobject_t &start, const hobject_t &end) final;
  void release_range() final;
  void scan_range(
    pg_shard_t target,
    const hobject_t &start,
    const hobject_t &end) final;
  void await_update(const eversion_t &version) final;
  void generate_and_submit_chunk_result(
    ScrubMap &map) final;
  void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) final;
};
  
};
