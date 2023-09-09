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

#define SIMPLE_EVENT(E) struct E : sc::event<E> {			\
    static constexpr std::string_view event_name = #E;			\
  }

#define VALUE_EVENT(E, T) struct E : sc::event<E> {			\
    static constexpr std::string_view event_name = #E;			\
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
  /// return id of local instance
  virtual pg_shard_t get_my_id() const = 0;

  /// return ids to scrub
  virtual const std::set<pg_shard_t> &get_ids_to_scrub() const = 0;

  template <typename F>
  void foreach_id_to_scrub(F &&f) {
    for (const auto &id : get_ids_to_scrub()) {
      std::invoke(f, id);
    }
  }

  template <typename F>
  void foreach_remote_id_to_scrub(F &&f) {
    auto not_me = [me = get_my_id()](const auto &i) {
      return me != i;
    };
    for (const auto &id : get_ids_to_scrub() | std::views::filter(not_me)) {
      std::invoke(f, id);
    }
  }

  /// return struct defining chunk validation rules
  virtual const chunk_validation_policy_t &get_policy() const = 0;

  struct request_range_result_t {
    hobject_t start;
    hobject_t end;
  };
  VALUE_EVENT(request_range_complete_t, request_range_result_t);
  virtual void request_range(
    const hobject_t &start) = 0;

  VALUE_EVENT(reserve_range_complete_t, eversion_t);
  virtual void reserve_range(
    const hobject_t &start,
    const hobject_t &end) = 0;

  SIMPLE_EVENT(await_update_complete_t);
  virtual void await_update(
    const eversion_t &version) = 0;

  /// cancel in progress or currently reserved range
  virtual void release_range() = 0;

  using scan_range_complete_value_t = std::pair<pg_shard_t, ScrubMap>;
  VALUE_EVENT(scan_range_complete_t, scan_range_complete_value_t);
  virtual void scan_range(
    pg_shard_t target,
    eversion_t version,
    const hobject_t &start,
    const hobject_t &end) = 0;

  SIMPLE_EVENT(generate_and_submit_chunk_result_complete_t);
  virtual void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) = 0;

  virtual void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) = 0;
};

struct Crash;
struct Inactive;

SIMPLE_EVENT(Reset);
SIMPLE_EVENT(StartScrub);

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
 */
class ScrubMachine
  : public sc::state_machine<ScrubMachine, Inactive> {
  static constexpr std::string_view full_name = "ScrubMachine";
public:
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
    static constexpr const char value[]{PN[PI]..., '/', CN[CI]...};
  };
  
  template <std::string_view const &PN, std::string_view const &CN>
  struct join {
    static constexpr std::string_view value = concat<
      PN, std::make_index_sequence<PN.size()>,
      CN, std::make_index_sequence<CN.size()>>::value;
  };

  /// Populated with ScrubMachine/.../Parent/Child for each state Child
  static constexpr std::string_view full_name =
    join<P::full_name, S::state_name>::value;

  template <typename C>
  explicit ScrubState(C ctx) : sc_base(ctx) {}

  auto &get_scrub_context() {
    return sc_base::template context<ScrubMachine>().context;
  }
};

struct Crash : ScrubState<Crash, ScrubMachine> {
  static constexpr std::string_view state_name = "Crash";
  explicit Crash(my_context ctx);
};

SIMPLE_EVENT(PrimaryActivate);
SIMPLE_EVENT(ReplicaActivate);
struct PrimaryActive;
struct ReplicaActive;
struct Inactive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "Inactive";
  explicit Inactive(my_context ctx);

  using reactions = boost::mpl::list<
    sc::transition<PrimaryActivate, PrimaryActive>,
    sc::transition<ReplicaActivate, ReplicaActive>,
    sc::custom_reaction<Reset>,
    sc::custom_reaction<StartScrub>
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
  explicit PrimaryActive(my_context ctx);

  bool local_reservation_held = false;
  std::set<pg_shard_t> remote_reservations_held;

  using reactions = boost::mpl::list<
    sc::transition<Reset, Inactive>,
    sc::custom_reaction<StartScrub>
    >;

  sc::result react(const StartScrub &) {
    return discard_event();
  }
};

struct Scrubbing;
struct AwaitScrub : ScrubState<AwaitScrub, PrimaryActive> {
  static constexpr std::string_view state_name = "AwaitScrub";
  explicit AwaitScrub(my_context ctx);

  using reactions = boost::mpl::list<
    sc::transition<StartScrub, Scrubbing>
    >;
};

struct ChunkState;
struct Scrubbing : ScrubState<Scrubbing, PrimaryActive, ChunkState> {
  static constexpr std::string_view state_name = "Scrubbing";
  explicit Scrubbing(my_context ctx);

  /// hobjects < current have been scrubbed
  hobject_t current;

  void advance_current(const hobject_t &next) {
    current = next;
  }
};

struct GetRange;
struct ChunkState : ScrubState<ChunkState, Scrubbing, GetRange> {
  static constexpr std::string_view state_name = "ChunkState";
  explicit ChunkState(my_context ctx);

  /// Current chunk includes objects in [range_start, range_end)
  boost::optional<ScrubContext::request_range_result_t> range;

  /// true once we have requested that the range be reserved
  bool range_reserved = false;

  /// version of last update for the reserved chunk
  eversion_t version;

  void exit();
};

struct GetRange : ScrubState<GetRange, ChunkState> {
  static constexpr std::string_view state_name = "GetRange";
  explicit GetRange(my_context ctx);

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::request_range_complete_t>
    >;

  sc::result react(const ScrubContext::request_range_complete_t &);
};

struct ScanRange;
struct WaitUpdate : ScrubState<WaitUpdate, ChunkState> {
  static constexpr std::string_view state_name = "WaitUpdate";
  explicit WaitUpdate(my_context ctx);

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::reserve_range_complete_t>
    >;

  sc::result react(const ScrubContext::reserve_range_complete_t &);
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

struct replica_scan_event_t {
  hobject_t start;
  hobject_t end;
  eversion_t version;
  bool deep = false;
};
VALUE_EVENT(ReplicaScan, replica_scan_event_t);
struct ReplicaIdle;
struct ReplicaActive :
    ScrubState<ReplicaActive, ScrubMachine, ReplicaIdle> {
  static constexpr std::string_view state_name = "ReplicaActive";
  explicit ReplicaActive(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<Reset, Inactive>,
    sc::custom_reaction<StartScrub>
    >;

  sc::result react(const StartScrub &) {
    return discard_event();
  }
};

struct ReplicaChunkState;
struct ReplicaIdle : ScrubState<ReplicaIdle, ReplicaActive> {
  static constexpr std::string_view state_name = "ReplicaIdle";
  explicit ReplicaIdle(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<ReplicaScan>
    >;

  sc::result react(const ReplicaScan &event) {
    post_event(event);
    return transit<ReplicaChunkState>();
  }
};

struct ReplicaWaitUpdate;
struct ReplicaChunkState : ScrubState<ReplicaChunkState, ReplicaActive, ReplicaWaitUpdate> {
  static constexpr std::string_view state_name = "ReplicaChunkState";
  explicit ReplicaChunkState(my_context ctx) : ScrubState(ctx) {}

  eversion_t version;
  hobject_t start, end;
  bool deep = false;

  using reactions = boost::mpl::list<
    sc::custom_reaction<ReplicaScan>
    >;

  sc::result react(const ReplicaScan &event);
};

struct ReplicaScanChunk;
struct ReplicaWaitUpdate : ScrubState<ReplicaWaitUpdate, ReplicaChunkState> {
  static constexpr std::string_view state_name = "ReplicaWaitUpdate";
  explicit ReplicaWaitUpdate(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<ReplicaScan>,
    sc::transition<ScrubContext::await_update_complete_t, ReplicaScanChunk>
    >;

  sc::result react(const ReplicaScan &event);
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
