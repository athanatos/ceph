// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <ranges>

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/deferral.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/in_state_reaction.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>

#include "common/hobject.h"
#include "common/hobject_fmt.h"
#include "crimson/common/log.h"
#include "osd/osd_types_fmt.h"
#include "scrub_validator.h"

namespace crimson::osd::scrub {

/* Development Notes
 *
 * Notes:
 * - We're leaving out all of the throttle waits.  We actually want to handle
 *   that using crimson's operation throttler machinery.
 *
 * TODOs:
 * - Leaving SnapMapper validation to later work
 *   - Note, each replica should validate and repair locally as the SnapMapper
 *     is meant to be a local index of the authoritative object contents
 * - Leaving preemption for later
 * - Leaving scheduling for later, for now the only way to trigger a scrub
 *   is via the OSD or PG commands.
 */

namespace sc = boost::statechart;

template <typename T>
struct scrub_event_t : sc::event<T> {
  std::string fmt_print() const { return T::event_name; }
};

#define SIMPLE_EVENT(E) struct E : scrub_event_t<E> {			\
    static constexpr const char * event_name = #E;			\
  }

#define VALUE_EVENT(E, T) struct E : scrub_event_t<E> {			\
    static constexpr const char * event_name = #E;			\
									\
    const T value;							\
									\
    template <typename... Args>						\
    E(Args&&... args) : value(std::forward<Args>(args)...) {}		\
    E(const E &) = default;						\
    E(E &&) = default;							\
    E &operator=(const E&) = default;					\
    E &operator=(E&&) = default;					\
  }

/**
 * ScrubContext
 *
 * Interface to external PG/OSD/IO machinery.
 *
 * Methods which may take time return immediately and define an event which
 * will be asyncronously delivered to the state machine with the result.  This
 * is a bit clumsy to use, but should render this component highly testable.
 *
 * Events sent as a completion to a ScrubContext interface method are defined
 * within ScrubContext.  Other events are defined within ScrubMachine.
 */
struct ScrubContext {
  /// return ids to scrub
  virtual const std::set<pg_shard_t> &get_ids_to_scrub() const = 0;

  /// iterates over each pg_shard_t to scrub
  template <typename F>
  void foreach_id_to_scrub(F &&f) {
    for (const auto &id : get_ids_to_scrub()) {
      std::invoke(f, id);
    }
  }

  /// return struct defining chunk validation rules
  virtual chunk_validation_policy_t get_policy() const = 0;

  /// notifies implementation of scrub start
  virtual void notify_scrub_start(bool deep) = 0;

  /// notifies implementation of scrub end
  virtual void notify_scrub_end(bool deep) = 0;

  /// requests range to scrub starting at start
  struct request_range_result_t {
    hobject_t start;
    hobject_t end;
  };
  VALUE_EVENT(request_range_complete_t, request_range_result_t);
  virtual void request_range(
    const hobject_t &start) = 0;

  /// reserves range [start, end)
  VALUE_EVENT(reserve_range_complete_t, eversion_t);
  virtual void reserve_range(
    const hobject_t &start,
    const hobject_t &end) = 0;

  /// waits until implementation has committed up to version
  SIMPLE_EVENT(await_update_complete_t);
  virtual bool await_update(
    const eversion_t &version) = 0;

  /// cancel in progress or currently reserved range
  virtual void release_range() = 0;

  /// scans [begin, end) on target as of version
  using scan_range_complete_value_t = std::pair<pg_shard_t, ScrubMap>;
  VALUE_EVENT(scan_range_complete_t, scan_range_complete_value_t);
  virtual void scan_range(
    pg_shard_t target,
    eversion_t version,
    bool deep,
    const hobject_t &start,
    const hobject_t &end) = 0;

  /// instructs implmentatino to scan [begin, end) and emit result to primary
  SIMPLE_EVENT(generate_and_submit_chunk_result_complete_t);
  virtual void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) = 0;

  /// notifies implementation of chunk scrub results
  virtual void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) = 0;

  /// notifies implementation of full scrub results
  virtual void emit_scrub_result(
    bool deep,
    object_stat_sum_t scrub_stats) = 0;

  /// get dpp instance for logging
  virtual DoutPrefixProvider &get_dpp() = 0;
};

struct Crash;
struct Inactive;

SIMPLE_EVENT(Reset);
struct start_scrub_event_t {
  bool deep = false;
};
VALUE_EVENT(StartScrub, start_scrub_event_t);
struct op_stat_event_t {
  hobject_t oid;
  object_stat_sum_t delta_stats;
};
VALUE_EVENT(OpStats, op_stat_event_t);

/**
 * ScrubMachine
 *
 * Manages orchestration of rados's distributed scrub process.
 *
 * There are two general ways in which ScrubMachine may need to release
 * resources:
 * - interval_change_t -- represents case where PG as a whole undergoes
 *   a distributed mapping change.  Distributed resources are released
 *   implicitely as remote PG instances receive the new map.  Local
 *   resources are still released by ScrubMachine via ScrubContext methods
 *   generally via state destructors
 * - otherwise, ScrubMachine is responsible for notifying remote PG
 *   instances via the appropriate ScrubContext methods again generally
 *   from state destructors.
 *
 * TODO: interval_change_t will be added with remote reservations.
 */
class ScrubMachine
  : public sc::state_machine<ScrubMachine, Inactive> {
public:
  static constexpr std::string_view full_name = "ScrubMachine";

  ScrubContext &context;
  ScrubMachine(ScrubContext &context) : context(context) {}
};

/**
 * ScrubState
 *
 * Template defining machinery/state common to all scrub state machine
 * states.
 */
template <typename S, typename P, typename... T>
struct ScrubState : sc::state<S, P, T...> {
  using sc_base = sc::state<S, P, T...>;

  /* machinery for populating a full_name member for each ScrubState with
   * ScrubMachine/.../ParentState/ChildState full_name */
  template <std::string_view const &PN, typename PI,
	    std::string_view const &CN, typename CI>
  struct concat;

  template <std::string_view const &PN, std::size_t... PI,
	    std::string_view const &CN, std::size_t... CI>
  struct concat<PN, std::index_sequence<PI...>, CN, std::index_sequence<CI...>> {
    static constexpr size_t value_size = PN.size() + CN.size() + 1;
    static constexpr const char value[value_size]{PN[PI]..., '/', CN[CI]...};
  };

  template <std::string_view const &PN, std::string_view const &CN>
  struct join {
    using conc = concat<
      PN, std::make_index_sequence<PN.size()>,
      CN, std::make_index_sequence<CN.size()>>;
    static constexpr std::string_view value{
      conc::value,
      conc::value_size
    };
  };

  /// Populated with ScrubMachine/.../Parent/Child for each state Child
  static constexpr std::string_view full_name =
    join<P::full_name, S::state_name>::value;

  template <typename C>
  explicit ScrubState(C ctx) : sc_base(ctx) {
    LOG_PREFIX(ScrubState::ScrubState);
    SUBDEBUGDPP(osd, "entering state {}", get_scrub_context().get_dpp(), full_name);
  }

  ~ScrubState() {
    LOG_PREFIX(ScrubState::~ScrubState);
    SUBDEBUGDPP(osd, "exiting state {}", get_scrub_context().get_dpp(), full_name);
  }

  auto &get_scrub_context() {
    return sc_base::template context<ScrubMachine>().context;
  }
};

struct Crash : ScrubState<Crash, ScrubMachine> {
  static constexpr std::string_view state_name = "Crash";
  explicit Crash(my_context ctx) : ScrubState(ctx) {
    ceph_abort("Crash state impossible");
  }

};

SIMPLE_EVENT(PrimaryActivate);
SIMPLE_EVENT(ReplicaActivate);
struct PrimaryActive;
struct ReplicaActive;
struct Inactive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "Inactive";
  explicit Inactive(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<PrimaryActivate, PrimaryActive>,
    sc::transition<ReplicaActivate, ReplicaActive>,
    sc::custom_reaction<Reset>,
    sc::custom_reaction<StartScrub>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const Reset &) {
    return discard_event();
  }
  sc::result react(const StartScrub &) {
    return discard_event();
  }
};

struct AwaitScrub;
struct PrimaryActive : ScrubState<PrimaryActive, ScrubMachine, AwaitScrub> {
  static constexpr std::string_view state_name = "PrimaryActive";
  explicit PrimaryActive(my_context ctx) : ScrubState(ctx) {}

  bool local_reservation_held = false;
  std::set<pg_shard_t> remote_reservations_held;

  using reactions = boost::mpl::list<
    sc::transition<Reset, Inactive>,
    sc::custom_reaction<StartScrub>,
    sc::custom_reaction<OpStats>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const StartScrub &event) {
    return discard_event();
  }

  sc::result react(const OpStats &) {
    return discard_event();
  }
};

struct Scrubbing;
struct AwaitScrub : ScrubState<AwaitScrub, PrimaryActive> {
  static constexpr std::string_view state_name = "AwaitScrub";
  explicit AwaitScrub(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<StartScrub>
    >;

  sc::result react(const StartScrub &event) {
    post_event(event);
    return transit<Scrubbing>();
  }
};

struct ChunkState;
struct Scrubbing : ScrubState<Scrubbing, PrimaryActive, ChunkState> {
  static constexpr std::string_view state_name = "Scrubbing";
  explicit Scrubbing(my_context ctx)
    : ScrubState(ctx), policy(get_scrub_context().get_policy()) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<StartScrub>,
    sc::custom_reaction<OpStats>
    >;

  chunk_validation_policy_t policy;

  /// hobjects < current have been scrubbed
  hobject_t current;

  /// true for deep scrub
  bool deep = false;

  /// stats for objects < current, maintained via OpStats
  object_stat_sum_t stats;

  void advance_current(const hobject_t &next) {
    current = next;
  }

  sc::result react(const StartScrub &event) {
    deep = event.value.deep;
    get_scrub_context().notify_scrub_start(deep);
    return discard_event();
  }

  void exit() {
    get_scrub_context().notify_scrub_end(deep);
  }

  sc::result react(const OpStats &event) {
    if (event.value.oid < current) {
      stats.add(event.value.delta_stats);
    }
    return discard_event();
  }
};

struct GetRange;
struct ChunkState : ScrubState<ChunkState, Scrubbing, GetRange> {
  static constexpr std::string_view state_name = "ChunkState";
  explicit ChunkState(my_context ctx) : ScrubState(ctx) {}

  /// Current chunk includes objects in [range_start, range_end)
  boost::optional<ScrubContext::request_range_result_t> range;

  /// true once we have requested that the range be reserved
  bool range_reserved = false;

  /// version of last update for the reserved chunk
  eversion_t version;

  void exit() {
    if (range_reserved) {
      get_scrub_context().release_range();
    }
  }
};

struct WaitUpdate;
struct GetRange : ScrubState<GetRange, ChunkState> {
  static constexpr std::string_view state_name = "GetRange";
  explicit GetRange(my_context ctx) : ScrubState(ctx) {
    get_scrub_context().request_range(context<Scrubbing>().current);
  }

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::request_range_complete_t>
    >;

  sc::result react(const ScrubContext::request_range_complete_t &event) {
    context<ChunkState>().range = event.value;
    return transit<WaitUpdate>();
  }
};

struct ScanRange;
struct WaitUpdate : ScrubState<WaitUpdate, ChunkState> {
  static constexpr std::string_view state_name = "WaitUpdate";
  explicit WaitUpdate(my_context ctx);

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::reserve_range_complete_t>
    >;

  sc::result react(const ScrubContext::reserve_range_complete_t &e) {
    context<ChunkState>().version = e.value;
    return transit<ScanRange>();
  }
};

struct ScanRange : ScrubState<ScanRange, ChunkState> {
  static constexpr std::string_view state_name = "ScanRange";
  explicit ScanRange(my_context ctx);

  scrub_map_set_t maps;
  unsigned waiting_on = 0;

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::scan_range_complete_t>
    >;

  sc::result react(const ScrubContext::scan_range_complete_t &);
};

struct ReplicaIdle;
struct ReplicaActive :
    ScrubState<ReplicaActive, ScrubMachine, ReplicaIdle> {
  static constexpr std::string_view state_name = "ReplicaActive";
  explicit ReplicaActive(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<Reset, Inactive>,
    sc::custom_reaction<StartScrub>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const StartScrub &) {
    return discard_event();
  }
};

struct replica_scan_event_t {
  hobject_t start;
  hobject_t end;
  eversion_t version;
  bool deep = false;
};
VALUE_EVENT(ReplicaScan, replica_scan_event_t);
struct ReplicaChunkState;
struct ReplicaIdle : ScrubState<ReplicaIdle, ReplicaActive> {
  static constexpr std::string_view state_name = "ReplicaIdle";
  explicit ReplicaIdle(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<ReplicaScan>
    >;

  sc::result react(const ReplicaScan &event) {
    LOG_PREFIX(ScrubState::ReplicaIdle::react(ReplicaScan));
    SUBDEBUGDPP(osd, "event.value: {}", get_scrub_context().get_dpp(), event.value);
    post_event(event);
    return transit<ReplicaChunkState>();
  }
};

struct ReplicaWaitUpdate;
struct ReplicaChunkState : ScrubState<ReplicaChunkState, ReplicaActive, ReplicaWaitUpdate> {
  static constexpr std::string_view state_name = "ReplicaChunkState";
  explicit ReplicaChunkState(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<ReplicaScan>
    >;

  replica_scan_event_t to_scan;

  sc::result react(const ReplicaScan &event) {
    LOG_PREFIX(ScrubState::ReplicaWaitUpdate::react(ReplicaScan));
    SUBDEBUGDPP(osd, "event.value: {}", get_scrub_context().get_dpp(), event.value);
    to_scan = event.value;
    if (get_scrub_context().await_update(event.value.version)) {
      post_event(ScrubContext::await_update_complete_t{});
    }
    return discard_event();
  }
};

struct ReplicaScanChunk;
struct ReplicaWaitUpdate : ScrubState<ReplicaWaitUpdate, ReplicaChunkState> {
  static constexpr std::string_view state_name = "ReplicaWaitUpdate";
  explicit ReplicaWaitUpdate(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::await_update_complete_t, ReplicaScanChunk>
    >;
};

struct ReplicaScanChunk : ScrubState<ReplicaScanChunk, ReplicaChunkState> {
  static constexpr std::string_view state_name = "ReplicaScanChunk";
  explicit ReplicaScanChunk(my_context ctx);

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::generate_and_submit_chunk_result_complete_t,
		   ReplicaIdle>
    >;
};

#undef SIMPLE_EVENT
#undef VALUE_EVENT

}

template <>
struct fmt::formatter<crimson::osd::scrub::replica_scan_event_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::osd::scrub::replica_scan_event_t &event,
	      FormatContext& ctx)
  {
    return fmt::format_to(
      ctx.out(),
      "replica_scan_event(start: {}, end: {}, version: {}, deep: {})",
      event.start, event.end, event.version, event.deep);
  }
};

template <>
struct fmt::formatter<
  crimson::osd::scrub::ScrubContext::request_range_result_t
  > {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const auto &range,
	      FormatContext& ctx)
  {
    return fmt::format_to(
      ctx.out(),
      "request_range_result_t({}~{})",
      range.start, range.end);
  }
};
