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

namespace crimson::osd::scrub {

#define SIMPLE_EVENT(E) struct E : boost::statechart::event<E> { \
    static constexpr event_name = #E;				 \
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

  
};

/**
 * ScrubState
 *
 * Template defining machinery/state common to all scrub state machine
 * states.
 */
template <typename S, typename P, typename... T>
struct ScrubState : boost::statechart::state<S, P, T...> {
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
  explicit ScrubState(C ctx) : boost::statechart::state<S, P, T...>(ctx) {}
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
  : public boost::statechart::state_machine<ScrubMachine, Inactive> {
  using reactions = boost::mpl::list<
    boost::statechart::transition<boost::statechart::event_base, Crash>
    >;

  static constexpr std::string_view full_name = "ScrubMachine";
public:

  /// Event to submit upon interval change, 
  SIMPLE_EVENT(interval_change_event_t);
};

struct Crash : ScrubState<Crash, ScrubMachine> {
  static constexpr std::string_view state_name = "Crash";

  explicit Crash(my_context ctx);
};
  
struct Inactive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "Inactive";
};

struct PrimaryActive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "PrimaryActive";
};

}
