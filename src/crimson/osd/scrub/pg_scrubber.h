// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "scrub_machine.h"

namespace crimson::osd {
class PG;
class ScrubScan;
}

namespace crimson::osd::scrub {

class PGScrubber : public crimson::BlockerT<PGScrubber>, ScrubContext {
  friend class ::crimson::osd::ScrubScan;

  template <typename T = void>
  using ifut =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;

  PG &pg;
  ScrubMachine machine;

  struct blocked_range_t {
    hobject_t begin;
    hobject_t end;
    seastar::shared_promise<> p;
  };
  std::optional<blocked_range_t> blocked;

  std::optional<eversion_t> waiting_for_update;

public:
  static constexpr const char *type_name = "PGScrubber";
  using Blocker = PGScrubber;
  void dump_detail(Formatter *f) const;

  static inline bool is_scrub_message(Message &m) {
    switch (m.get_type()) {
    case MSG_OSD_REP_SCRUB:
    case MSG_OSD_REP_SCRUBMAP:
      return true;
    default:
      return false;
    }
    return false;
  }

  PGScrubber(PG &pg) : pg(pg), machine(*this) {}

  void on_primary_active_clean();
  void on_replica_activate();
  void on_interval_change();

  void on_log_update(eversion_t v);

  void handle_scrub_requested();

  void handle_scrub_message(Message &m);

  ifut<> wait_scrub(
    PGScrubber::BlockingEvent::TriggerI&& trigger,
    const hobject_t &hoid);

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
  void reserve_range(const hobject_t &start, const hobject_t &end) final;
  void release_range() final;
  void scan_range(
    pg_shard_t target,
    const hobject_t &start,
    const hobject_t &end) final;
  void await_update(const eversion_t &version) final;
  void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) final;
  void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) final;
};
  
};
