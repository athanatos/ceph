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

  /**
   * request_local_reservation
   *
   * Asyncronously request a local reservation on primary osd.  Implementation
   * must signal completion by submitting a
   * request_local_reservation_complete_t event unless first canceled.
   *
   * ScrubMachine is responsible for releasing the reservation even in the
   * event of an interval_change_event_t.
   */
  SIMPLE_EVENT(request_local_reservation_complete_t);
  virtual void request_local_reservation() = 0;

  /// cancel in progress or current local reservation
  virtual void cancel_local_reservation() = 0;

  /**
   * replica_request_local_reservation
   *
   * Asyncronously request a local reservation a replica.  Implementation
   * must signal completion by submitting a
   * replica_request_local_reservation_complete_t event unless first canceled.
   *
   * ScrubMachine is responsible for releasing the reservation even in the
   * event of an interval_change_event_t.
   */
  SIMPLE_EVENT(replica_request_local_reservation_complete_t);
  virtual void replica_request_local_reservation() = 0;

  /// cancel in progress or current local reservation
  virtual void replica_cancel_local_reservation() = 0;

  /// signal to primary that reservation was successful
  virtual void replica_confirm_reservation() = 0;


  /**
   * request_remote_reservations
   *
   * Asyncronously request reservations on a remote peer.  Implementation
   * must signal completion by submitting a
   * request_remote_reservation_complete_t event with the completed peer unless
   * first canceled.
   *
   * ScrubMachine is not responsible for releasing the remote reservation in the
   * event of an interval_change_event_t as the remote peer will do so
   * upon receipt of the new map.  In other cases, ScrubMachine is responsible
   * for releasing the reservation.
   */
  VALUE_EVENT(request_remote_reservations_complete_t, pg_shard_t);
  virtual void request_remote_reservation(pg_shard_t target) = 0;

  /// cancel in progress or current remote reservations
  virtual void cancel_remote_reservation(pg_shard_t target) = 0;

  struct request_range_result_t {
    hobject_t start;
    hobject_t end;
  };
  VALUE_EVENT(request_range_complete_t, request_range_result_t);
  virtual void request_range(
    const hobject_t &start) = 0;

  SIMPLE_EVENT(reserve_range_complete_t);
  virtual void reserve_range(
    const hobject_t &start,
    const hobject_t &end) = 0;

  /// cancel in progress or currently reserved range
  virtual void release_range() = 0;

  using scan_range_complete_value_t = std::pair<pg_shard_t, ScrubMap>;
  VALUE_EVENT(scan_range_complete_t, scan_range_complete_value_t);
  virtual void scan_range(
    pg_shard_t target,
    const hobject_t &start,
    const hobject_t &end) = 0;

  virtual void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) = 0;
};

struct Crash;
struct Inactive;

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
  using reactions = boost::mpl::list<
    sc::transition<sc::event_base, Crash>
    >;

  static constexpr std::string_view full_name = "ScrubMachine";
public:

  /// Event to submit upon interval change, 
  SIMPLE_EVENT(interval_change_event_t);

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
  
struct Inactive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "Inactive";
};

struct GetLocalReservation;
struct PrimaryActive : ScrubState<PrimaryActive, ScrubMachine, GetLocalReservation> {
  static constexpr std::string_view state_name = "PrimaryActive";

  bool local_reservation_held = false;
  bool remote_reservations_held = false;

  void exit();
};

struct GetRemoteReservations;
struct GetLocalReservation : ScrubState<GetLocalReservation, PrimaryActive> {
  static constexpr std::string_view state_name = "GetLocalReservation";

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::request_local_reservation_complete_t,
				  GetRemoteReservations>
    >;

  explicit GetLocalReservation(my_context ctx);
};

struct Scrubbing;
struct GetRemoteReservations : ScrubState<GetRemoteReservations, PrimaryActive> {
  static constexpr std::string_view state_name = "GetRemoteReservations";
  unsigned waiting_on = 0;

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::request_remote_reservations_complete_t>
    >;

  explicit GetRemoteReservations(my_context ctx);

  sc::result react(const ScrubContext::request_remote_reservations_complete_t &);
};

struct ChunkState;
struct Scrubbing : ScrubState<Scrubbing, PrimaryActive, ChunkState> {
  static constexpr std::string_view state_name = "Scrubbing";

  /// hobjects < current have been scrubbed
  hobject_t current;

  explicit Scrubbing(my_context ctx);

  void advance_current(const hobject_t &next) {
    current = next;
  }
};

struct GetRange;
struct ChunkState : ScrubState<ChunkState, Scrubbing, GetRange> {
  static constexpr std::string_view state_name = "ChunkState";

  /// Current chunk includes objects in [range_start, range_end)
  boost::optional<ScrubContext::request_range_result_t> range;

  /// true once we have requested that the range be reserved
  bool range_reserved = false;

  explicit ChunkState(my_context ctx);
  void exit();
};

struct GetRange : ScrubState<GetRange, ChunkState> {
  static constexpr std::string_view state_name = "GetRange";

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::request_range_complete_t>
    >;

  explicit GetRange(my_context ctx);

  sc::result react(const ScrubContext::request_range_complete_t &);
};

struct ScanRange;
struct ReserveRange : ScrubState<ReserveRange, ChunkState> {
  static constexpr std::string_view state_name = "ReserveRange";

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::reserve_range_complete_t, ScanRange>
    >;

  explicit ReserveRange(my_context ctx);
};

struct ScanRange : ScrubState<ScanRange, ChunkState> {
  static constexpr std::string_view state_name = "ScanRange";
  scrub_map_set_t maps;
  unsigned waiting_on = 0;

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::scan_range_complete_t>
    >;

  explicit ScanRange(my_context ctx);

  sc::result react(const ScrubContext::scan_range_complete_t &);
};

struct ReplicaGetLocalReservation;
struct ReplicaActive : ScrubState<ReplicaActive, ScrubMachine, ReplicaGetLocalReservation> {
  static constexpr std::string_view state_name = "ReplicaActive";

  bool reservation_held = false;

  void exit();
};

struct ReplicaAwaitScan;
struct ReplicaGetLocalReservation : ScrubState<ReplicaGetLocalReservation, ReplicaActive> {
  static constexpr std::string_view state_name = "ReplicaGetLocalReservation";

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::replica_request_local_reservation_complete_t>
    >;

  explicit ReplicaGetLocalReservation(my_context ctx);

  sc::result react(const ScrubContext::replica_request_local_reservation_complete_t &);
};

struct ReplicaAwaitScan : ScrubState<ReplicaGetLocalReservation, ReplicaActive> {
  static constexpr std::string_view state_name = "ReplicaAwaitScan";
};


#undef SIMPLE_EVENT
#undef VALUE_EVENT

}
