// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <algorithm>
#include <array>
#include <set>
#include <vector>
#include <limits>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/future-util.hh>

#include "include/ceph_assert.h"
#include "include/utime.h"
#include "common/Formatter.h"
#include "common/Clock.h"
#include "crimson/common/interruptible_future.h"

namespace ceph {
  class Formatter;
}

namespace crimson {

using registry_hook_t = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

class Operation;
class Blocker;

/**
 * Provides an abstraction for registering and unregistering a blocker
 * for the duration of a future becoming available.
 */
template <typename Fut>
class blocking_future_detail {
// just as a scaffolding for the transition from blocking_future
public:
  friend class Blocker;
  Blocker *blocker;
  Fut fut;
  blocking_future_detail(Blocker *b, Fut &&f)
    : blocker(b), fut(std::move(f)) {}

  template <typename V, typename... U>
  friend blocking_future_detail<seastar::future<V>>
  make_ready_blocking_future(U&&... args);

  template <typename V, typename Exception>
  friend blocking_future_detail<seastar::future<V>>
  make_exception_blocking_future(Exception&& e);

  template <typename U>
  friend blocking_future_detail<seastar::future<>> join_blocking_futures(U &&u);

  template <typename InterruptCond, typename T>
  friend blocking_future_detail<
    ::crimson::interruptible::interruptible_future<InterruptCond>>
  join_blocking_interruptible_futures(T&& t);

  template <typename U>
  friend class blocking_future_detail;

public:
  template <typename F>
  auto then(F &&f) && {
    using result = decltype(std::declval<Fut>().then(f));
    return blocking_future_detail<seastar::futurize_t<result>>(
      blocker,
      std::move(fut).then(std::forward<F>(f)));
  }
  template <typename InterruptCond, typename F>
  auto then_interruptible(F &&f) && {
    using result = decltype(std::declval<Fut>().then_interruptible(f));
    return blocking_future_detail<
      typename ::crimson::interruptible::interruptor<
	InterruptCond>::template futurize<result>::type>(
      blocker,
      std::move(fut).then_interruptible(std::forward<F>(f)));
  }
};

template <typename T=void>
using blocking_future = blocking_future_detail<seastar::future<T>>;

template <typename InterruptCond, typename T = void>
using blocking_interruptible_future = blocking_future_detail<
  ::crimson::interruptible::interruptible_future<InterruptCond, T>>;

template <typename InterruptCond, typename V, typename U>
blocking_interruptible_future<InterruptCond, V>
make_ready_blocking_interruptible_future(U&& args) {
  return blocking_interruptible_future<InterruptCond, V>(
    nullptr,
    seastar::make_ready_future<V>(std::forward<U>(args)));
}

template <typename InterruptCond, typename V, typename Exception>
blocking_interruptible_future<InterruptCond, V>
make_exception_blocking_interruptible_future(Exception&& e) {
  return blocking_interruptible_future<InterruptCond, V>(
    nullptr,
    seastar::make_exception_future<InterruptCond, V>(e));
}

template <typename V=void, typename... U>
blocking_future_detail<seastar::future<V>> make_ready_blocking_future(U&&... args) {
  return blocking_future<V>(
    nullptr,
    seastar::make_ready_future<V>(std::forward<U>(args)...));
}

template <typename V, typename Exception>
blocking_future_detail<seastar::future<V>>
make_exception_blocking_future(Exception&& e) {
  return blocking_future<V>(
    nullptr,
    seastar::make_exception_future<V>(e));
}

/**
 * Provides an interface for dumping diagnostic information about
 * why a particular op is not making progress.
 */
class Blocker {
public:
  template <typename T>
  blocking_future<T> make_blocking_future(seastar::future<T> &&f) {
    return blocking_future<T>(this, std::move(f));
  }

  template <typename InterruptCond, typename T>
  blocking_interruptible_future<InterruptCond, T>
  make_blocking_future(
      crimson::interruptible::interruptible_future<InterruptCond, T> &&f) {
    return blocking_interruptible_future<InterruptCond, T>(
      this, std::move(f));
  }
  template <typename InterruptCond, typename T = void>
  blocking_interruptible_future<InterruptCond, T>
  make_blocking_interruptible_future(seastar::future<T> &&f) {
    return blocking_interruptible_future<InterruptCond, T>(
	this,
	::crimson::interruptible::interruptor<InterruptCond>::make_interruptible(
	  std::move(f)));
  }

  void dump(ceph::Formatter *f) const;
  virtual ~Blocker() = default;

private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;
  virtual const char *get_type_name() const = 0;
};

// the main template. by default an operation has no extenral
// event handler (the empty tuple). specializing the template
// allows to define backends on per-operation-type manner.
// NOTE: basically this could be a function but C++ disallows
// differentiating return type among specializations.
template <class T>
struct EventBackendRegistry {
  template <typename...> static constexpr bool always_false = false;

  static std::tuple<> get_backends() {
    static_assert(always_false<T>, "Registry specialization not found");
    return {};
  }
};

template <class T>
struct Event {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  template <class OpT, class... Args>
  void trigger(OpT&& op, Args&&... args) {
    that()->internal_backend.handle(*that(),
                                    std::forward<OpT>(op),
                                    std::forward<Args>(args)...);

    // let's call `handle()` for concrete event type from each single
    // of our backends. the order in the registry matters.
    std::apply([&, //args=std::forward_as_tuple(std::forward<Args>(args)...),
		this] (auto... backend) {
      (..., backend.handle(*that(),
                           std::forward<OpT>(op),
                           std::forward<Args>(args)...));
    }, EventBackendRegistry<std::decay_t<OpT>>::get_backends());
  }
};


// simplest event type for recording things like beginning or end
// of TrackableOperation's life.
template <class T>
struct TimeEvent : Event<T> {
  struct Backend {
    // `T` is passed solely to let implementations to discriminate
    // basing on the type-of-event.
    virtual void handle(T&, const Operation&) = 0;
  };

  // for the sake of dumping ops-in-flight.
  struct InternalBackend final : Backend {
    void handle(T&, const Operation&) override {
      timestamp = ceph_clock_now();
    }
  private:
    utime_t timestamp;
  } internal_backend;
};


template <typename T>
class BlockerT : public Blocker {
public:
  struct BlockingEvent : Event<BlockingEvent> {
    using Blocker = std::decay_t<T>;

    struct Backend {
      // `T` is based solely to let implementations to discriminate
      // basing on the type-of-event.
      virtual void handle(typename T::BlockingEvent&, const Operation&, const T&) = 0;
    };

    struct InternalBackend : Backend {
      void handle(typename T::BlockingEvent&,
                  const Operation&,
                  const T& blocker) override {
        this->timestamp = ceph_clock_now();
        this->blocker = &blocker;
      }

      utime_t timestamp;
      const T* blocker;
    } internal_backend;

    // we don't want to make any BlockerT to be aware and coupled with
    // an operation. to not templatize an entire path from an op to
    // a blocker, type erasuring is used.
    struct TriggerI {
      template <class FutureT>
      decltype(auto) maybe_record_blocking(FutureT&& fut, const T& blocker) {
        if (!fut.available()) {
          // a full blown call via vtable. that's the cost for templatization
	  // avoidance. anyway, most of the things actually have the type
	  // knowledge.
          record_event(blocker);
	}
	return std::forward<FutureT>(fut);
      }

      virtual ~TriggerI() = default;
    protected:
      virtual void record_event(const T& blocker) = 0;
    };

    template <class OpT>
    struct Trigger : TriggerI {
      Trigger(BlockingEvent& event, const OpT& op) : event(event), op(op) {}

      template <class FutureT>
      decltype(auto) maybe_record_blocking(FutureT&& fut, const T& blocker) {
        if (!fut.available()) {
          // no need for the dynamic dispatch! if we're lucky, a compiler
	  // should collapse all these abstractions into a bunch of movs.
          this->Trigger::record_event(blocker);
        }
	return std::forward<FutureT>(fut);
      }

      void record_unblocking(const T& blocker) {
	assert(event.internal_backend.blocker == &blocker);
        event.internal_backend.blocker = nullptr;
      }
    protected:
      void record_event(const T& blocker) override {
        event.trigger(op, blocker);
      }

      BlockingEvent& event;
      const OpT& op;
    };
  };

  virtual ~BlockerT() = default;
  template <class TriggerT, class... Args>
  decltype(auto) track_blocking(TriggerT&& trigger, Args&&... args) {
    return std::forward<TriggerT>(trigger).maybe_record_blocking(
      std::forward<Args>(args)..., static_cast<const T&>(*this));
  }

private:
  const char *get_type_name() const final {
    return static_cast<const T*>(this)->type_name;
  }
};

template <class T>
struct AggregateBlockingEvent {
  struct TriggerI {
    template <class FutureT>
    decltype(auto) maybe_record_blocking(FutureT&& fut,
		    			 const typename T::Blocker& blocker) {
      // AggregateBlockingEvent is supposed to be used on relatively cold
      // paths (recovery), so we don't need to worry about the dynamic
      // polymothps / dynamic memory's overhead.
      return create_part_trigger()->maybe_record_blocking(
	std::move(fut), blocker);
    }

    virtual std::unique_ptr<typename T::TriggerI> create_part_trigger() = 0;
    virtual ~TriggerI() = default;
  };

  template <class OpT>
  struct Trigger : TriggerI {
    Trigger(AggregateBlockingEvent& event, const OpT& op)
      : event(event), op(op) {}

    std::unique_ptr<typename T::TriggerI> create_part_trigger() override {
      return std::make_unique<typename T::template Trigger<OpT>>(
	event.events.emplace_back(), op);
    }

  private:
    AggregateBlockingEvent& event;
    const OpT& op;
  };

private:
  std::vector<T> events;
  template <class OpT>
  friend class Trigger;
};

class AggregateBlocker : public BlockerT<AggregateBlocker> {
  std::vector<Blocker*> parent_blockers;
public:
  AggregateBlocker(std::vector<Blocker*> &&parent_blockers)
    : parent_blockers(std::move(parent_blockers)) {}
  static constexpr const char *type_name = "AggregateBlocker";
private:
  void dump_detail(ceph::Formatter *f) const final;
};

template <typename T>
blocking_future<> join_blocking_futures(T &&t) {
  std::vector<Blocker*> blockers;
  blockers.reserve(t.size());
  for (auto &&bf: t) {
    blockers.push_back(bf.blocker);
    bf.blocker = nullptr;
  }
  auto agg = std::make_unique<AggregateBlocker>(std::move(blockers));
  return agg->make_blocking_future(
    seastar::parallel_for_each(
      std::forward<T>(t),
      [](auto &&bf) {
	return std::move(bf.fut);
      }).then([agg=std::move(agg)] {
	return seastar::make_ready_future<>();
      }));
}

template <typename InterruptCond, typename T>
blocking_interruptible_future<InterruptCond>
join_blocking_interruptible_futures(T&& t) {
  std::vector<Blocker*> blockers;
  blockers.reserve(t.size());
  for (auto &&bf: t) {
    blockers.push_back(bf.blocker);
    bf.blocker = nullptr;
  }
  auto agg = std::make_unique<AggregateBlocker>(std::move(blockers));
  return agg->make_blocking_future(
    ::crimson::interruptible::interruptor<InterruptCond>::parallel_for_each(
      std::forward<T>(t),
      [](auto &&bf) {
	return std::move(bf.fut);
      }).then_interruptible([agg=std::move(agg)] {
	return seastar::make_ready_future<>();
      }));
}

using operation_id_t = uint64_t;
constexpr operation_id_t INVALID_OPERATION_ID =
  std::numeric_limits<operation_id_t>::max();

/**
 * Common base for all crimson-osd operations.  Mainly provides
 * an interface for registering ops in flight and dumping
 * diagnostic information.
 */
class Operation : public boost::intrusive_ref_counter<
  Operation, boost::thread_unsafe_counter> {
 public:
  operation_id_t get_id() const {
    return id;
  }

  virtual unsigned get_type() const = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(std::ostream &) const = 0;

  void dump(ceph::Formatter *f) const;
  void dump_brief(ceph::Formatter *f) const;
  virtual ~Operation() = default;

 private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

  registry_hook_t registry_hook;

  operation_id_t id = 0;
  void set_id(operation_id_t in_id) {
    id = in_id;
    _set_id(in_id);
  }

  /**
   * _set_id
   *
   * Implementations should use to set any internal members, notably
   * PipelineHandle::set_id.
   */
  virtual void _set_id(operation_id_t in_id) {}

  friend class OperationRegistryI;
  template <size_t>
  friend class OperationRegistryT;
};
using OperationRef = boost::intrusive_ptr<Operation>;

std::ostream &operator<<(std::ostream &, const Operation &op);

/**
 * Maintains a set of lists of all active ops.
 */
class OperationRegistryI {
  using op_list_member_option = boost::intrusive::member_hook<
    Operation,
    registry_hook_t,
    &Operation::registry_hook
    >;

  friend class Operation;
  seastar::timer<seastar::lowres_clock> shutdown_timer;
  seastar::promise<> shutdown;

protected:
  virtual void do_register(Operation *op) = 0;
  virtual bool registries_empty() const = 0;

public:
  using op_list = boost::intrusive::list<
    Operation,
    op_list_member_option,
    boost::intrusive::constant_time_size<false>>;

  template <typename T, typename... Args>
  typename T::IRef create_operation(Args&&... args) {
    typename T::IRef op = new T(std::forward<Args>(args)...);
    do_register(&*op);
    return op;
  }

  seastar::future<> stop() {
    shutdown_timer.set_callback([this] {
      if (registries_empty()) {
	shutdown.set_value();
	shutdown_timer.cancel();
      }
    });
    shutdown_timer.arm_periodic(
      std::chrono::milliseconds(100/*TODO: use option instead*/));
    return shutdown.get_future();
  }
};


template <size_t NUM_REGISTRIES>
class OperationRegistryT : public OperationRegistryI {
  std::array<
    op_list,
    NUM_REGISTRIES
  > registries;

  std::array<
    uint64_t,
    NUM_REGISTRIES
  > op_id_counters = {};

protected:
  void do_register(Operation *op) final {
    const auto op_type = op->get_type();
    registries[op_type].push_back(*op);
    op->set_id(++op_id_counters[op_type]);
  }

  bool registries_empty() const final {
    return std::all_of(registries.begin(),
		       registries.end(),
		       [](auto& opl) {
			 return opl.empty();
		       });
  }
public:
  template <size_t REGISTRY_INDEX>
  const op_list& get_registry() const {
    static_assert(
      REGISTRY_INDEX < std::tuple_size<decltype(registries)>::value);
    return registries[REGISTRY_INDEX];
  }
};

class PipelineExitBarrierI {
public:
  using Ref = std::unique_ptr<PipelineExitBarrierI>;

  /// Waits for exit barrier
  virtual seastar::future<> wait() = 0;

  /// Releases pipeline stage, can only be called after wait
  virtual void exit() = 0;

  /// Releases pipeline resources without waiting on barrier
  virtual void cancel() = 0;

  /// Must ensure that resources are released, likely by calling cancel()
  virtual ~PipelineExitBarrierI() {}
};

template <class T>
class PipelineStageIT : public BlockerT<T> {
public:
  virtual seastar::future<PipelineExitBarrierI::Ref> enter(
    operation_id_t op_id
  ) = 0;
};

class PipelineHandle {
  PipelineExitBarrierI::Ref barrier;

  auto wait_barrier() {
    return barrier ? barrier->wait() : seastar::now();
  }

  operation_id_t parent_operation_id = INVALID_OPERATION_ID;
public:
  PipelineHandle() = default;

  PipelineHandle(const PipelineHandle&) = delete;
  PipelineHandle(PipelineHandle&&) = default;
  PipelineHandle &operator=(const PipelineHandle&) = delete;
  PipelineHandle &operator=(PipelineHandle&&) = default;

  void set_id(operation_id_t id) { parent_operation_id = id; }

  /**
   * Returns a future which unblocks when the handle has entered the passed
   * OrderedPipelinePhase.  If already in a phase, enter will also release
   * that phase after placing itself in the queue for the next one to preserve
   * ordering.
   */
  template <typename T>
  blocking_future<> enter(T &t) {
    /* Strictly speaking, we probably want the blocker to be registered on
     * the previous stage until wait_barrier() resolves and on the next
     * until enter() resolves, but blocking_future will need some refactoring
     * to permit that.  TODO
     */
    assert(parent_operation_id != INVALID_OPERATION_ID);
    return t.make_blocking_future(
      wait_barrier().then([this, &t] {
	auto fut = t.enter(parent_operation_id);
	exit();
	return std::move(fut).then([this](auto &&barrier_ref) {
	  barrier = std::move(barrier_ref);
	  return seastar::now();
	});
      })
    );
  }

  template <typename OpT, typename T>
  seastar::future<>
  enter(T &t, typename T::BlockingEvent::template Trigger<OpT>&& f) {
    assert(parent_operation_id != INVALID_OPERATION_ID);
    return wait_barrier().then([this, &t, f=std::move(f)] () mutable {
      auto fut = f.maybe_record_blocking(t.enter(parent_operation_id), t);
      exit();
      return std::move(fut).then(
        [this, &t, f=std::move(f)](auto &&barrier_ref) mutable {
        barrier = std::move(barrier_ref);
	// TODO: move this to exit. It would require dynamic memory + TriggerI
	// or, alternatively, static_ptr
        f.record_unblocking(t);
        return seastar::now();
      });
    });
  }

  /**
   * Completes pending exit barrier without entering a new one.
   */
  seastar::future<> complete() {
    auto ret = wait_barrier();
    barrier.reset();
    return ret;
  }

  /**
   * Exits current phase, skips exit barrier, should only be used for op
   * failure.  Permitting the handle to be destructed as the same effect.
   */
  void exit() {
    barrier.reset();
  }

};

/**
 * Ensures that at most one op may consider itself in the phase at a time.
 * Ops will see enter() unblock in the order in which they tried to enter
 * the phase.  entering (though not necessarily waiting for the future to
 * resolve) a new phase prior to exiting the previous one will ensure that
 * the op ordering is preserved.
 */
template <class T>
class OrderedExclusivePhaseT : public PipelineStageIT<T> {
  operation_id_t held_by = INVALID_OPERATION_ID;
  void set_held_by(operation_id_t id) {
    assert(held_by == INVALID_OPERATION_ID);
    held_by = id;
  }
  void clear_held_by() {
    assert(held_by != INVALID_OPERATION_ID);
    held_by = INVALID_OPERATION_ID;
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_unsigned("held_by", held_by);
  }

  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedExclusivePhaseT *phase;
    operation_id_t id;
  public:
    ExitBarrier(OrderedExclusivePhaseT *phase, operation_id_t id)
      : phase(phase), id(id) {}

    seastar::future<> wait() final {
      return seastar::now();
    }

    void exit() final {
      if (phase) {
	assert(phase->held_by == id);
	phase->exit();
	phase = nullptr;
      }
    }

    void cancel() final {
      exit();
    }

    ~ExitBarrier() final {
      cancel();
    }
  };

  void exit() {
    clear_held_by();
    mutex.unlock();
  }

public:
  seastar::future<PipelineExitBarrierI::Ref> enter(operation_id_t id) final {
    return mutex.lock().then([this, id] {
      set_held_by(id);
      return PipelineExitBarrierI::Ref(new ExitBarrier{this, id});
    });
  }

private:
  seastar::shared_mutex mutex;
};

// TODO: drop this after migrating to the new event tracking infrastructure.
struct OrderedExclusivePhase : OrderedExclusivePhaseT<OrderedExclusivePhase> {
  OrderedExclusivePhase(const char *type_name) : type_name(type_name) {}
  const char * type_name;
};

/**
 * Permits multiple ops to inhabit the stage concurrently, but ensures that
 * they will proceed to the next stage in the order in which they called
 * enter.
 */
template <class T>
class OrderedConcurrentPhaseT : public PipelineStageIT<T> {
  void dump_detail(ceph::Formatter *f) const final {}

  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedConcurrentPhaseT *phase;
    std::optional<seastar::future<>> barrier;
  public:
    ExitBarrier(
      OrderedConcurrentPhaseT *phase,
      seastar::future<> &&barrier) : phase(phase), barrier(std::move(barrier)) {}

    seastar::future<> wait() final {
      assert(phase);
      assert(barrier);
      auto ret = std::move(*barrier);
      barrier = std::nullopt;
      return ret;
    }

    void exit() final {
      if (barrier) {
	static_cast<void>(
	  std::move(*barrier).then([phase=this->phase] { phase->mutex.unlock(); }));
	barrier = std::nullopt;
	phase = nullptr;
      }
      if (phase) {
	phase->mutex.unlock();
	phase = nullptr;
      }
    }

    void cancel() final {
      exit();
    }

    ~ExitBarrier() final {
      cancel();
    }
  };

public:
  seastar::future<PipelineExitBarrierI::Ref> enter(operation_id_t) final {
    return seastar::make_ready_future<PipelineExitBarrierI::Ref>(
      new ExitBarrier{this, mutex.lock()});
  }

private:
  seastar::shared_mutex mutex;
};

struct OrderedConcurrentPhase : OrderedConcurrentPhaseT<OrderedConcurrentPhase> {
  OrderedConcurrentPhase(const char *type_name) : type_name(type_name) {}
  const char * type_name;
};

/**
 * Imposes no ordering or exclusivity at all.  Ops enter without constraint and
 * may exit in any order.  Useful mainly for informational purposes between
 * stages with constraints.
 */
template <class T>
class UnorderedStageT : public PipelineStageIT<T> {
  void dump_detail(ceph::Formatter *f) const final {}

  class ExitBarrier final : public PipelineExitBarrierI {
  public:
    ExitBarrier() = default;

    seastar::future<> wait() final {
      return seastar::now();
    }

    void exit() final {}

    void cancel() final {}

    ~ExitBarrier() final {}
  };

public:
  seastar::future<PipelineExitBarrierI::Ref> enter(operation_id_t) final {
    return seastar::make_ready_future<PipelineExitBarrierI::Ref>(
      new ExitBarrier);
  }
};

struct UnorderedStage : UnorderedStageT<UnorderedStage> {
  UnorderedStage(const char *type_name) : type_name(type_name) {}
  const char * type_name;
};

}
