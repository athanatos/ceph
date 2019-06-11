// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_mutex.hh>
#include <seastar/core/future.hh>

#include <vector>
#include <set>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/Formatter.h"

namespace ceph::osd {

enum class OperationTypeCode {
  client_request = 0,
  peering_event,
  compound_peering_request,
  last_op
};

static constexpr const char* const OP_NAMES[] = {
  "client_write",
  "peering_event",
  "compound_peering_request"
};

class OperationRegistry;

using registry_hook_t = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

class Operation;
class Blocker;

template <typename... T>
class blocking_future {
  friend class Operation;
  friend class Blocker;
  Blocker *blocker = nullptr;
  seastar::future<T...> fut;
  blocking_future(Blocker *b, seastar::future<T...> &&f)
    : blocker(b), fut(std::move(f)) {}

  template <typename... V, typename... U>
  friend blocking_future<V...> make_ready_blocking_future(U&&... args);
};

template <typename... V, typename... U>
blocking_future<V...> make_ready_blocking_future(U&&... args) {
  return blocking_future<V...>(
    nullptr,
    seastar::make_ready_future<V...>(std::forward<U>(args)...));
}

class Blocker {
protected:
  virtual void dump_detail(Formatter *f) const = 0;

public:
  template <typename... T>
  blocking_future<T...> get_blocking_future(seastar::future<T...> &&f) {
    return blocking_future(this, std::move(f));
  }

  void dump(Formatter *f) const;

  virtual const char *get_type_name() const = 0;

  virtual ~Blocker() = default;
};

template <typename T>
class BlockerT : public Blocker {
public:
  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~BlockerT() = default;
};

class Operation : public boost::intrusive_ref_counter<
  Operation, boost::thread_unsafe_counter> {
  friend class OperationRegistry;
  registry_hook_t registry_hook;

  std::set<Blocker*> blockers;
  uint64_t id = 0;
  void set_id(uint64_t in_id) {
    id = in_id;
  }
protected:
  virtual void dump_detail(Formatter *f) const = 0;

public:
  uint64_t get_id() const {
    return id;
  }

  virtual OperationTypeCode get_type() const = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(std::ostream &) const = 0;

  void add_blocker(Blocker *b) {
    blockers.insert(b);
  }

  void clear_blocker(Blocker *b) {
    blockers.erase(b);
  }

  template <typename... T>
  seastar::future<T...> with_blocking_future(blocking_future<T...> &&f) {
    if (f.fut.available() || f.fut.failed()) {
      return std::move(f.fut);
    }
    ceph_assert(f.blocker);
    add_blocker(f.blocker);
    return std::move(f.fut).then_wrapped([this, blocker=f.blocker](auto &&arg) {
      clear_blocker(blocker);
      return std::move(arg);
    });
  }

  void dump(Formatter *f);
  void dump_brief(Formatter *f);
  virtual ~Operation() = default;
};
using OperationRef = boost::intrusive_ptr<Operation>;

std::ostream &operator<<(std::ostream &, const Operation &op);

template <typename T>
class OperationT : public Operation {

protected:
  virtual void dump_detail(Formatter *f) const = 0;

public:
  static constexpr const char *type_name = OP_NAMES[static_cast<int>(T::type)];
  using IRef = boost::intrusive_ptr<T>;

  OperationTypeCode get_type() const final {
    return T::type;
  }

  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~OperationT() = default;
};

class OperationRegistry {
  friend class Operation;
  using op_list_member_option = boost::intrusive::member_hook<
    Operation,
    registry_hook_t,
    &Operation::registry_hook
    >;
  using op_list = boost::intrusive::list<
    Operation,
    op_list_member_option,
    boost::intrusive::constant_time_size<false>>;

  std::vector<op_list> registries = std::vector<op_list>(
    static_cast<int>(OperationTypeCode::last_op));

  std::vector<uint64_t> op_id_counters = std::vector<uint64_t>(
    static_cast<int>(OperationTypeCode::last_op), 0);
public:
  template <typename T, typename... Args>
  typename T::IRef create_operation(Args&&... args) {
    typename T::IRef op = new T(std::forward<Args>(args)...);
    registries[static_cast<int>(T::type)].push_back(*op);
    op->set_id(op_id_counters[static_cast<int>(T::type)]++);
    return op;
  }
};

class OrderedPipelinePhase : public Blocker {
  const char * name;

protected:
  virtual void dump_detail(Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

public:
  seastar::shared_mutex mutex;

  class Handle {
    OrderedPipelinePhase *phase = nullptr;

  public:
    Handle() = default;

    Handle(const Handle&) = delete;
    Handle(Handle&&) = delete;
    Handle &operator=(const Handle&) = delete;
    Handle &operator=(Handle&&) = delete;

    void exit();
    blocking_future<> enter(OrderedPipelinePhase &phase);

    ~Handle();
  };

  OrderedPipelinePhase(const char *name) : name(name) {}
};

}
