// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

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

namespace crimson::osd::scrub {

namespace sc = boost::statechart;

#define SIMPLE_EVENT(E) struct E : sc::event<E> {	\
    static constexpr std::string_view event_name = #E;			\
  }

#define VALUE_EVENT(E, T) struct E : sc::event<E> {	\
    static constexpr std::string_view event_name = #E;			\
    									\
    const T value;							\
									\
    template <typename... Args>						\
    E(Args&&... args) : value(std::forward<Args>(args)...) {}		\
    E(const E &) = default;						\
    E(E &&) = default;							\
    E &operator=(const E&) = default;					\
    E &operator=(E&&) = default;						\
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

  /**
   * request_local_reservation
   *
   * Asyncronously request a reservation on the local osd.  Implementation
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
   * request_remote_reservations
   *
   * Asyncronously request reservations on the remote peers.  Implementation
   * must signal completion by submitting a
   * request_remote_reservation_complete_t event unless first canceled.
   *
   * ScrubMachine is not responsible for releasing the remote reservations in the
   * event of an interval_change_event_t as the remote peers will do so
   * upon receipt of the new map.  In other cases, ScrubMachine is responsible
   * for releasing the reservation.
   */
  SIMPLE_EVENT(request_remote_reservations_complete_t);
  virtual void request_remote_reservations() = 0;

  /// cancel in progress or current remote reservations
  virtual void cancel_remote_reservations() = 0;

  struct request_range_result_t {
    const hobject_t start;
    const hobject_t end;
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

  struct scan_range_result_t {
    // TODO
  };
  VALUE_EVENT(scan_range_complete_t, scan_range_result_t);
  virtual void scan_range(
    const hobject_t &start,
    const hobject_t &end) = 0;
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

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::request_remote_reservations_complete_t,
		   Scrubbing>
    >;

  explicit GetRemoteReservations(my_context ctx);
};

struct ChunkState;
struct Scrubbing : ScrubState<Scrubbing, PrimaryActive, ChunkState> {
  static constexpr std::string_view state_name = "Scrubbing";

  /// hobjects < current have been scrubbed
  hobject_t current;

  explicit Scrubbing(my_context ctx);
};

struct GetRange;
struct ChunkState : ScrubState<ChunkState, Scrubbing, GetRange> {
  static constexpr std::string_view state_name = "ChunkState";

  /// Current chunk includes objects in [range_start, range_end)
  hobject_t range_start, range_end;

  /// true once we have requested that the range be reserved
  bool range_reserved = false;

  /// chunk scan contents, populated after ScanRange
  boost::optional<ScrubContext::scan_range_result_t> scan_results;

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

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::scan_range_complete_t>
    >;

  explicit ScanRange(my_context ctx);

  sc::result react(const ScrubContext::scan_range_complete_t &);
};

#undef SIMPLE_EVENT
#undef VALUE_EVENT

}
