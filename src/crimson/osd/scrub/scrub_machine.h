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

template <typename E>
struct scrub_event_t : boost::statechart::event<E> {
};

/**
 * ScrubContext
 *
 * Interface to external PG/OSD/IO machinery.
 *
 * Methods which may take time return immediately and define an event which
 * will be asyncronously delivered to the state machine with the result.  This
 * is a bit clumsy to use, but should render this component highly testable.
 */
struct ScrubContext {
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
class ScrubMachine
  : public boost::statechart::state_machine<ScrubMachine, Inactive> {
  using reactions = boost::mpl::list<
    boost::statechart::transition<boost::statechart::event_base, Crash>
    >;

  static constexpr std::string_view full_name = "ScrubMachine";
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
