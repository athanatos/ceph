// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * *Very* heavily inspired by
 * http://docs.seastar-project.org/master/future_8hh_source.html
 */

#ifndef CEPH_FUTURE_H
#define CEPH_FUTURE_H

#include <memory>
#include <future>
#include <type_traits>
#include <memory>
#include <list>

#include <boost/optional.hpp>
#include <boost/noncopyable.hpp>

namespace Ceph {

namespace FutureDetail {

template<typename T, typename ErrT> class Result;
template<typename T, typename ErrT> class RemoteEscrow;
template<typename T, typename ErrT> class LocalPromise;

template <typename T, typename ErrT>
class FutureState {
  friend class Result<T, ErrT>;
  friend class RemoteEscrow<T, ErrT>;
  friend class LocalPromise<T, ErrT>;

  class ErrMarker {};
  class ValMarker {};
  enum class Status {
    INVALID,
    ERR,
    VAL,
  } status;
  union {
    T val;
    ErrT err;
  };
  FutureState() : status(Status::INVALID) {}

  template <typename... A>
  FutureState(ValMarker, A&&... a)
    : status(Status::VAL), val(std::forward<A>(a)...) {}

  template <typename... A>
  FutureState(ErrMarker, A&&... a)
    : status(Status::ERR), err(std::forward<A>(a)...) {}

  bool is_valid() const { return status != Status::INVALID; }
public:
  using ErrorType = ErrT;
  using ValueType = T;
  FutureState(const FutureState &f) = delete;
  FutureState(FutureState &&f) : status(f.status) {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      new (&err) ErrT(std::move(f.err));
      f.err.~ErrT();
      f.status = Status::INVALID;
      break;
    case Status::VAL:
      new (&val) T(std::move(f.val));
      f.val.~T();
      f.status = Status::INVALID;
      break;
    default:
      assert(0 == "impossible");
    }
  }
  FutureState &operator=(const FutureState &f) = delete;
  FutureState &operator=(FutureState &&f) {
    if (this != &f) {
      this->~FutureState();
      new (this) FutureState(std::move(f));
    }
    return *this;
  }

  template <typename... A>
  static FutureState<T, ErrT> make_value(A&&... a) {
    return FutureState<T, ErrT>(ValMarker(), std::forward<A>(a)...);
  }

  template <typename... A>
  static FutureState<T, ErrT> make_error(A&&... a) {
    return FutureState<T, ErrT>(ErrMarker(), std::forward<A>(a)...);
  }

  bool is_error() const {
    assert(status != Status::INVALID);
    return status == Status::ERR;
  }
  bool is_val() const {
    assert(status != Status::INVALID);
    return status == Status::VAL;
  }

  T get_value() {
    assert(status == Status::VAL);
    T ret(std::move(val));
    val.~T();
    status = Status::INVALID;
    return ret;
  }

  ErrT get_error() {
    assert(status == Status::ERR);
    ErrT ret(std::move(err));
    err.~ErrT();
    status = Status::INVALID;
    return ret;
  }

  ~FutureState() {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      err.~ErrT();
      break;
    case Status::VAL:
      val.~T();
      break;
    default:
      assert(0 == "impossible");
      break;
    }
    status = Status::INVALID;
  }
};

template <typename ErrT>
class FutureState<void, ErrT> {
  friend class Result<void, ErrT>;
  friend class RemoteEscrow<void, ErrT>;
  friend class LocalPromise<void, ErrT>;

  class ErrMarker {};
  class ValMarker {};
  enum class Status {
    INVALID,
    ERR,
    VAL,
  } status;
  union {
    ErrT err;
  };
  FutureState() : status(Status::INVALID) {}

  template <typename... A>
  FutureState(ValMarker, A&&... a)
    : status(Status::VAL) {}

  template <typename... A>
  FutureState(ErrMarker, A&&... a)
    : status(Status::ERR), err(std::forward<A>(a)...) {}

  bool is_valid() const { return status != Status::INVALID; }
public:
  using ErrorType = ErrT;
  using ValueType = void;
  FutureState(const FutureState &f) = delete;
  FutureState(FutureState &&f) : status(f.status) {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      new (&err) ErrT(std::move(f.err));
      f.err.~ErrT();
      f.status = Status::INVALID;
      break;
    case Status::VAL:
      f.status = Status::INVALID;
      break;
    default:
      assert(0 == "impossible");
    }
  }
  FutureState &operator=(const FutureState &f) = delete;
  FutureState &operator=(FutureState &&f) {
    if (this != &f) {
      this->~FutureState();
      new (this) FutureState(std::move(f));
    }
    return *this;
  }

  static FutureState<void, ErrT> make_value() {
    return FutureState<void, ErrT>(ValMarker());
  }

  template <typename... A>
  static FutureState<void, ErrT> make_error(A&&... a) {
    return FutureState<void, ErrT>(ErrMarker(), std::forward<A>(a)...);
  }

  bool is_error() const {
    assert(status != Status::INVALID);
    return status == Status::ERR;
  }
  bool is_val() const {
    assert(status != Status::INVALID);
    return status == Status::VAL;
  }

  void get_value() {
    assert(status == Status::VAL);
    status = Status::INVALID;
  }

  ErrT get_error() {
    assert(status == Status::ERR);
    ErrT ret(std::move(err));
    err.~ErrT();
    status = Status::INVALID;
    return ret;
  }

  ~FutureState() {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      err.~ErrT();
      break;
    case Status::VAL:
      break;
    default:
      assert(0 == "impossible");
      break;
    }
    status = Status::INVALID;
  }
};


template <typename T>
class FutureState<T, void> {
  friend class Result<T, void>;
  friend class RemoteEscrow<T, void>;
  friend class LocalPromise<T, void>;

  class ErrMarker {};
  class ValMarker {};
  enum class Status {
    INVALID,
    ERR,
    VAL,
  } status;
  union {
    T val;
  };
  FutureState() : status(Status::INVALID) {}

  template <typename... A>
  FutureState(ValMarker, A&&... a)
    : status(Status::VAL), val(std::forward<A>(a)...) {}

  FutureState(ErrMarker)
    : status(Status::ERR) {}

  bool is_valid() const { return status != Status::INVALID; }
public:
  using ErrorType = void;
  using ValueType = T;
  FutureState(const FutureState &f) = delete;
  FutureState(FutureState &&f) : status(f.status) {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      f.status = Status::INVALID;
      break;
    case Status::VAL:
      new (&val) T(std::move(f.val));
      f.val.~T();
      f.status = Status::INVALID;
      break;
    default:
      assert(0 == "impossible");
    }
  }
  FutureState &operator=(const FutureState &f) = delete;
  FutureState &operator=(FutureState &&f) {
    if (this != &f) {
      this->~FutureState();
      new (this) FutureState(std::move(f));
    }
    return *this;
  }

  template <typename... A>
  static FutureState<T, void> make_value(A&&... a) {
    return FutureState<T, void>(ValMarker(), std::forward<A>(a)...);
  }

  static FutureState<T, void> make_error() {
    return FutureState<T, void>(ErrMarker());
  }

  bool is_error() const {
    assert(status != Status::INVALID);
    return status == Status::ERR;
  }
  bool is_val() const {
    assert(status != Status::INVALID);
    return status == Status::VAL;
  }

  T get_value() {
    assert(status == Status::VAL);
    T ret(std::move(val));
    val.~T();
    status = Status::INVALID;
    return ret;
  }

  void get_error() {
    assert(status == Status::ERR);
    status = Status::INVALID;
    return;
  }

  ~FutureState() {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::ERR:
      break;
    case Status::VAL:
      val.~T();
      break;
    default:
      assert(0 == "impossible");
      break;
    }
    status = Status::INVALID;
  }
};

template <>
class FutureState<void, void> {
  friend class Result<void, void>;
  friend class RemoteEscrow<void, void>;
  friend class LocalPromise<void, void>;

  enum class Status {
    INVALID,
    ERR,
    VAL,
  } status;

  FutureState() : status(Status::INVALID) {}

  bool is_valid() const { return status != Status::INVALID; }
public:
  using ErrorType = void;
  using ValueType = void;

  static FutureState<void, void> make_value() {
    FutureState<void, void> ret;
    ret.status = Status::VAL;
    return ret;
  }

  static FutureState<void, void> make_error() {
    FutureState<void, void> ret;
    ret.status = Status::ERR;
    return ret;
  }

  bool is_error() const {
    assert(status != Status::INVALID);
    return status == Status::ERR;
  }
  bool is_val() const {
    assert(status != Status::INVALID);
    return status == Status::VAL;
  }

  void get_value() {
    assert(status == Status::VAL);
    status = Status::INVALID;
  }

  void get_error() {
    assert(status == Status::ERR);
    status = Status::INVALID;
  }
};

template <typename T, typename ErrT>
class Result;

template <typename T, typename ErrT>
class LocalPromise {
  friend class Result<T, ErrT>;
  Result<T, ErrT> *target = nullptr;


public:
  LocalPromise() = default;
  LocalPromise(LocalPromise &&);
  LocalPromise &operator=(LocalPromise &&r) {
    if (this != &r) {
      this->~LocalPromise();
      new (this) LocalPromise(std::move(r));
    }
    return *this;
  }

  void pass_state(FutureState<T, ErrT> &&f);

  template <typename... A>
  void fulfill(A&&... a) {
    pass_state(FutureState<T, ErrT>::make_value(std::forward<A>(a)...));
  }
  template <typename... A>
  void error(A&&... a) {
    pass_state(FutureState<T, ErrT>::make_error(std::forward<A>(a)...));
  }

  bool valid() const { return target; }

  ~LocalPromise();
};

template <typename T, typename ErrT>
struct RemoteEscrow {
  RemoteEscrow(const RemoteEscrow &) = delete;
  RemoteEscrow &operator=(const RemoteEscrow &) = delete;
  RemoteEscrow(RemoteEscrow &&) = delete;
  RemoteEscrow &operator=(RemoteEscrow &&) = delete;

  std::mutex l;
  using guard = std::unique_lock<std::mutex>;
  std::condition_variable c;
  bool valid;
  FutureState<T, ErrT> s;

  RemoteEscrow() : valid(true) {}


  template <typename... U>
  void set_val(U&&... u) {
    guard g(l);
    s = FutureState<T, ErrT>::make_value(std::forward<U...>(u)...);
    c.notify_one();
  }
  template <typename... U>
  void set_err(U&&... u) {
    guard g(l);
    s = FutureState<T, ErrT>::make_error(std::forward<U...>(u)...);
    c.notify_one();
  }
  bool is_ready() {
    guard g(l);
    return s.is_valid();
  }
  void wait() {
    guard g(l);
    while (!s.is_valid())
      c.wait(g);
  }
  FutureState<T, ErrT> get_state() {
    guard g(l);
    return std::move(s);
  }
  bool is_error() {
    guard g(l);
    return s.is_error();
  }
  void invalidate() {
    guard g(l);
    valid = false;
  }
  bool is_valid() {
    guard g(l);
    return valid;
  }
};
template<typename T, typename ErrT>
using RemoteEscrowRef = std::shared_ptr<RemoteEscrow<T, ErrT>>;

template <typename T, typename ErrT>
class RemotePromise {
  friend class Result<T, ErrT>;
  RemoteEscrowRef<T, ErrT> target;
public:
  RemotePromise() = default;
  RemotePromise(const RemotePromise &) = delete;
  RemotePromise(RemotePromise &&p) : target(std::move(p.target)) {}
  RemotePromise& operator=(RemotePromise &&p) {
    if (&p != this) {
      this->~RemotePromise();
      new (this) RemotePromise(std::move(p));
    }
    return *this;
  }
  template <typename... U>
  void fulfill(U&&... u) {
    assert(target);
    target->set_val(std::forward<U...>(u)...);
    target.reset();
  }
  template <typename... U>
  void error(U&&... u) {
    assert(target);
    target->set_err(std::forward<U...>(u)...);
    target.reset();
  }
  bool valid() {
    return target && target->is_valid();
  }
  ~RemotePromise() {
    if (target)
      target->invalidate();
  }
};

template <typename T, typename ErrT>
class Result {
  friend class LocalPromise<T, ErrT>;
  enum class Status {
    INVALID,
    REMOTE,
    LOCAL,
  } status;
  struct local_t {
    LocalPromise<T, ErrT> *lp;
    FutureState<T, ErrT> result;
  };
  using remote_t = RemoteEscrowRef<T, ErrT>;
  union {
    local_t local;
    remote_t remote;
  };
  void invalidate() {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::REMOTE:
      remote->invalidate();
      remote.~remote_t();
      status = Status::INVALID;
      break;
    case Status::LOCAL:
      if (local.lp) {
	assert(local.lp->target == this);
	local.lp->target = nullptr;
	local.lp = nullptr;
      }
      local.~local_t();
      status = Status::INVALID;
      break;
    default:
      assert(0 == "impossible value");
      break;
    }
  }
public:
  Result() : status(Status::INVALID) {}
  Result(const Result &r) = delete;
  Result(Result &&r) : status(r.status) {
    switch (status) {
    case Status::INVALID:
      break;
    case Status::REMOTE:
      new (&remote) remote_t(std::move(r.remote));
      r.remote.~remote_t();
      r.status = Status::INVALID;
      break;
    case Status::LOCAL:
      new (&local) local_t(std::move(r.local));
      r.local.~local_t();
      r.status = Status::INVALID;
      if (local.lp) {
	assert(local.lp->target == &r);
	local.lp->target = this;
      }
      break;
    default:
      assert(0 == "impossible value");
    }
  }
  Result &operator=(Result &&r) {
    if (this != &r) {
      this->~Result();
      new (this) Result(std::move(r));
    }
    return *this;
  }
  Result(FutureState<T, ErrT> &&f) :
    status(Status::LOCAL),
    local{nullptr, std::move(f)} {}
  ~Result() { invalidate(); }
  bool valid() {
    switch (status) {
    case Status::INVALID:
      return false;
    case Status::REMOTE:
      assert(remote);
      return remote->is_valid();
    case Status::LOCAL:
      assert((local.lp && !local.result.is_valid()) ||
	     (!local.lp && local.result.is_valid()));
      return true;
    default:
      assert(0 == "impossible state");
    }
    assert(0 == "unreachable");
    return false;
  }
  bool ready() {
    switch (status) {
    case Status::INVALID:
      assert(0 == "object is invalid");
      return false;
    case Status::REMOTE:
      assert(remote);
      return remote->is_ready();
    case Status::LOCAL:
      assert((local.lp && !local.result.is_valid()) ||
	     (!local.lp && local.result.is_valid()));
      return local.result.is_valid();
    default:
      assert(0 == "impossible state");
    }
    assert(0 == "unreachable");
    return false;
  }
  RemotePromise<T, ErrT> get_remote_promise() {
    assert(!valid());
    RemotePromise<T, ErrT> ret;
    status = Status::REMOTE;
    new (&remote) remote_t(new RemoteEscrow<T, ErrT>());
    ret.target = remote;
    return ret;
  }
  LocalPromise<T, ErrT> get_local_promise() {
    assert(!valid());
    LocalPromise<T, ErrT> ret;
    status = Status::LOCAL;
    ret.target = this;
    new (&local) local_t();
    local.lp = &ret;
    return ret;
  }
  void forward(LocalPromise<T, ErrT> &&p) {
    assert(valid());
    assert(p.target);
    Result<T, ErrT> *t = p.target;
    p.target->~Result();
    assert(!p.valid());

    new (t) Result(std::move(*this));
    assert(!valid());
  }
  bool is_error() {
    assert(valid());
    assert(ready());
    switch(status) {
    case Status::INVALID:
      break;
    case Status::REMOTE:
      return remote->is_error();
    case Status::LOCAL:
      return local.result.is_error();
    }
    assert(0 == "unreachable");
    return false;
  }
  FutureState<T, ErrT> get() {
    FutureState<T, ErrT> ret;
    assert(valid());
    assert(ready());
    switch(status) {
    case Status::INVALID:
      assert(0 == "unreachable");
      break;
    case Status::REMOTE:
      ret = remote->get_state();
      break;
    case Status::LOCAL:
      ret = std::move(local.result);
      break;
    }
    invalidate();
    return ret;
  }
  void wait() {
    switch(status) {
    case Status::INVALID:
      assert(0 == "invalid Result");
      break;
    case Status::REMOTE:
      remote->wait();
      assert(ready());
      break;
    case Status::LOCAL:
      if (!ready())
	assert(0 == "waiting on local completion -- deadlock");
      break;
    default:
      assert(0 == "invalid status");
      break;
    }
  }
};

class Transformer {
public:
  using Ref = typename std::unique_ptr<Transformer>;

  virtual bool ready() = 0;
  virtual void wait() = 0;

  // invalidates *this
  virtual std::list<Ref> run() && = 0;

  virtual ~Transformer() {}
};

template <typename U, typename Func, typename ErrT>
class TransformerImpl : boost::noncopyable, public Transformer {
public:
  Func f;
  Result<U, ErrT> src;
  TransformerImpl(Func &&f, Result<U, ErrT> &&_src) :
    f(std::move(f)), src(std::move(_src)) {
    assert(src.valid());
  }

  bool ready() {
    return src.ready();
  }

  std::list<Ref> run() && {
    return f(src.get());
  }

  void wait() {
    src.wait();
  }
};

template <typename U, typename Func, typename ErrT>
Transformer *make_transformer(Func &&f, Result<U, ErrT> &&u) {
  return new TransformerImpl<U, Func, ErrT>(std::move(f), std::move(u));
}

template <typename T, typename ErrT>
class Future;

template <typename T, typename ErrT>
struct EMakeFuture {
private:
  using FutureType = Future<T, ErrT>;
  using FutureStateType = FutureState<T, ErrT>;
public:
  static FutureType from_error(FutureType &&x) {
    return std::move(x);
  }
  static FutureType from_error(FutureStateType &&x) {
    return FutureType(std::move(x));;
  }
  static FutureType from_error(ErrT &&x) {
    return FutureType::make_ready_error(std::move(x));
  }
};

template <typename T>
struct EMakeFuture<T, void> {
private:
  using FutureType = Future<T, void>;
  using FutureStateType = FutureState<T, void>;
  using WrappedType = T;
public:
  static FutureType from_error(FutureType &&x) {
    return std::move(x);
  }
  static FutureType from_error(FutureStateType &&x) {
    return FutureType(std::move(x));;
  }
  static FutureType from_error() {
    return FutureType::make_ready_error();
  }
};

template <typename T, typename ErrT>
struct VMakeFuture {
private:
  using FutureType = Future<T, ErrT>;
  using FutureStateType = FutureState<T, ErrT>;
public:
  static FutureType from_value(FutureType &&x) {
    return std::move(x);
  }
  static FutureType from_value(FutureStateType &&x) {
    return FutureType(std::move(x));;
  }
  static FutureType from_value(T &&x) {
    return FutureType::make_ready_value(std::move(x));
  }
};

template <typename ErrT>
struct VMakeFuture<void, ErrT> {
private:
  using FutureType = Future<void, ErrT>;
  using FutureStateType = FutureState<void, ErrT>;
public:
  static FutureType from_value(FutureType &&x) {
    return std::move(x);
  }
  static FutureType from_value(FutureStateType &&x) {
    return FutureType(std::move(x));;
  }
  static FutureType from_value() {
    return FutureType::make_ready_value();
  }
};

template <typename T, typename ErrT>
struct Futurize : VMakeFuture<T, ErrT>, EMakeFuture<T, ErrT> {
  using FutureType = Future<T, ErrT>;
  using FutureStateType = FutureState<T, ErrT>;
  using WrappedType = T;
  using ErrorType = ErrT;
};

template <typename T, typename ErrT2, typename ErrT>
struct Futurize<Future<T, ErrT2>, ErrT>
  : VMakeFuture<T, ErrT2>, EMakeFuture<T, ErrT2> {
  using FutureType = Future<T, ErrT2>;
  using FutureStateType = FutureState<T, ErrT2>;
  using WrappedType = T;
  using ErrorType = ErrT2;
};

template <typename T, typename ErrT2, typename ErrT>
struct Futurize<FutureState<T, ErrT2>, ErrT>
  : VMakeFuture<T, ErrT2>, EMakeFuture<T, ErrT2> {
  using FutureType = Future<T, ErrT2>;
  using FutureStateType = FutureState<T, ErrT2>;
  using WrappedType = T;
  using ErrorType = ErrT2;
};

template <typename ErrT2, typename T, typename ErrT>
struct ErrorFuturize {
  using ErrorType = ErrT2;
};
template <typename T2, typename ErrT2, typename T, typename ErrT>
struct ErrorFuturize<Future<T2, ErrT2>, T, ErrT> {
  using ErrorType = ErrT2;
};
template <typename T2, typename ErrT2, typename T, typename ErrT>
struct ErrorFuturize<FutureState<T2, ErrT2>, T, ErrT> {
  using ErrorType = ErrT2;
};

template <typename F, typename A>
struct RetTypeOf {
  using Type = typename std::result_of<F(A)>::type;
};
template <typename F>
struct RetTypeOf<F, void> {
  using Type = typename std::result_of<F()>::type;
};

template<typename Fut, typename Func, typename ...A>
typename std::enable_if<
  std::is_void<typename std::result_of<Func(A...)>::type>::value,
  typename Fut::FutureType>::type
apply(Fut &&fut, Func &&f, A&&... a) {
  f(std::forward<A>(a)...);
  return Fut::from_value();
}
template<typename Fut, typename Func, typename ...A>
typename std::enable_if<
  !std::is_void<typename std::result_of<Func(A...)>::type>::value,
  typename Fut::FutureType>::type
apply(Fut &&fut, Func &&f, A&&... a) {
  return Fut::from_value(f(std::forward<A>(a)...));
}

template<typename Fut, typename Func, typename FSType>
typename std::enable_if<
  !std::is_void<typename FSType::ErrorType>::value &&
  !std::is_void<
    typename std::result_of<Func(typename FSType::ErrorType)>::type>::value,
  typename Fut::FutureType>::type
apply_error(Fut &&fut, Func &&f, FSType &&fs) {
  return Fut::from_error(f(fs.get_error()));
}
template<typename T, typename Func, typename FSType>
typename std::enable_if<
  std::is_void<typename FSType::ErrorType>::value &&
  std::is_void<typename std::result_of<Func()>::type>::value,
  Future<T, void>>::type
apply_error(Futurize<T, void> &&fut, Func &&f, FSType &&fs) {
  f();
  return Future<T, void>::make_ready_error();
}
template<typename T, typename Func, typename FSType>
typename std::enable_if<
  !std::is_void<typename FSType::ErrorType>::value &&
  std::is_void<
    typename std::result_of<Func(typename FSType::ErrorType)>::type>::value,
  Future<T, void>>::type
apply_error(Futurize<T, void> &&fut, Func &&f, FSType &&fs) {
  f(fs.get_error());
  return Future<T, void>::make_ready_error();
}
template<typename T, typename Func, typename FSType>
typename std::enable_if<
  std::is_void<typename FSType::ErrorType>::value &&
  !std::is_void<
    typename std::result_of<Func()>::type>::value,
  Future<T, void>>::type
apply_error(Futurize<T, void> &&fut, Func &&f, FSType &&fs) {
  return Futurize<T, void>::from_error(f());
}

template<typename Fut, typename Func, typename FSType>
typename std::enable_if<
  !std::is_void<typename FSType::ValueType>::value &&
  !std::is_void<
    typename std::result_of<Func(typename FSType::ValueType)>::type>::value,
  typename Fut::FutureType>::type
apply_value(Fut &&fut, Func &&f, FSType &&fs) {
  return Fut::from_value(f(fs.get_value()));
}
template<typename ErrT, typename Func, typename FSType>
typename std::enable_if<
  std::is_void<typename FSType::ValueType>::value &&
  std::is_void<
    typename std::result_of<Func()>::type>::value,
  Future<void, ErrT>>::type
apply_value(Futurize<void, ErrT> &&fut, Func &&f, FSType &&fs) {
  f();
  return Future<void, ErrT>::make_ready_value();
}
template<typename ErrT, typename Func, typename FSType>
typename std::enable_if<
  !std::is_void<typename FSType::ValueType>::value &&
  std::is_void<
    typename std::result_of<Func(typename FSType::ValueType)>::type>::value,
  Future<void, ErrT>>::type
apply_value(Futurize<void, ErrT> &&fut, Func &&f, FSType &&fs) {
  f(fs.get_value());
  Future<void, ErrT> ret;
  ret.get_promise().fulfill();
  return ret;
}
template<typename ErrT, typename Func, typename FSType>
typename std::enable_if<
  std::is_void<typename FSType::ValueType>::value &&
  !std::is_void<typename std::result_of<Func()>::type>::value,
  Future<void, ErrT>>::type
apply_value(Futurize<void, ErrT> &&fut, Func &&f, FSType &&fs) {
  return Futurize<void, ErrT>::from_error(f());
}

template <typename T, typename ErrT = int>
class Future : boost::noncopyable {
  template <typename U, typename ErrU>
  friend class Future;

  Result<T, ErrT> inbox;
  std::list<Transformer::Ref> blockers;

  Future(Result<T, ErrT> &&inbox, std::list<Transformer::Ref> &&blockers) :
    inbox(std::move(inbox)), blockers(std::move(blockers)) {}


  struct ErrMarker {};
  struct ValMarker {};
  template <typename... A>
  Future(ValMarker, A&&... a)
    : inbox(std::move(FutureState<T, ErrT>::make_value(
			std::forward<A>(a)...))) {}
  template <typename... A>
  Future(ErrMarker, A&&... a)
    : inbox(std::move(FutureState<T, ErrT>::make_error(
			std::forward<A>(a)...))) {}
public:
  Future() {}
  Future(FutureState<T, ErrT> &&f) : inbox(std::move(f)) {}
  Future(Future &&f) :
    inbox(std::move(f.inbox)), blockers(std::move(f.blockers)) {}

  Future &operator=(Future &&f) {
    this->~Future();
    new (this) Future(std::move(f));
    return *this;
  }

  template <typename... A>
  static Future<T, ErrT> make_ready_value(A&&... a) {
    return Future<T, ErrT>(ValMarker(), std::forward<A>(a)...);
  }
  template <typename... A>
  static Future<T, ErrT> make_ready_error(A&&... a) {
    return Future<T, ErrT>(ErrMarker(), std::forward<A>(a)...);
  }

  template <typename... A>
  void set_error(A&&... a) {
    assert(!valid());
    inbox = FutureState<T, ErrT>::make_error(std::forward<A>(a)...);
  }

  bool valid() {
    return inbox.valid();
  }

  bool blocked() {
    return !((blockers.empty() && inbox.ready()) ||
	     blockers.front()->ready());
  }

  bool ready() {
    return inbox.ready();
  }

  bool is_error() {
    assert(ready());
    return inbox.is_error();
  }

  RemotePromise<T, ErrT> get_promise() {
    return inbox.get_remote_promise();
  }

  FutureState<T, ErrT> get() {
    assert(ready());
    return inbox.get();
  }

  T get_value() {
    assert(ready() && !is_error());
    return inbox.get().get_value();
  }

  ErrT get_error() {
    assert(ready() && is_error());
    return inbox.get().get_error();
  }

  template <typename Func>
  auto then(Func &&f) ->
    typename Futurize<
      typename RetTypeOf<Func, FutureState<T, ErrT>>::Type, ErrT
      >::FutureType {
    using futurator = Futurize<
      typename RetTypeOf<Func, FutureState<T, ErrT>>::Type, ErrT>;
    using futuretype = typename futurator::FutureType;
    using wrapped_type = typename futurator::WrappedType;
    using error_type = typename futurator::ErrorType;
    using promise = LocalPromise<wrapped_type, error_type>;
    using result = Result<wrapped_type, error_type>;

    assert(valid());

    struct Cont {
      Func f;
      promise p;
      Cont(Func &&f, promise &&p) : f(std::move(f)), p(std::move(p)) {}
      std::list<Transformer::Ref> operator()(FutureState<T, ErrT> &&fs) {
	futuretype fut(apply(futurator(), std::move(f), std::move(fs)));
	if (fut.ready()) {
	  p.pass_state(std::move(fut.get()));
	  return std::list<Transformer::Ref>();
	} else {
	  fut.inbox.forward(std::move(p));
	  return std::move(fut.blockers);
	}
      }
    };

    result res;
    promise p(res.get_local_promise());
    blockers.push_back(
      Transformer::Ref(
	make_transformer(
	  Cont(std::move(f), std::move(p)),
	  std::move(inbox))));

    futuretype ret(std::move(res), std::move(blockers));
    return ret;
  }

  template <typename Func>
  auto then_ignore_error(Func &&f) ->
    typename Futurize<typename RetTypeOf<Func, T>::Type, ErrT>::FutureType {
    using futurator = Futurize<typename RetTypeOf<Func, T>::Type, ErrT>;
    struct Cont {
      Func f;
      Cont(Func &&f) : f(std::move(f)) {}
      typename futurator::FutureType operator()(FutureState<T, ErrT> &&fs) {
	if (fs.is_error())
	  return typename futurator::FutureType();
	else
	  return apply_value(futurator(), std::move(f), std::move(fs));
      }
    };
    return then(Cont(std::move(f)));
  }

  template <typename Func, typename Erf>
  auto then_with_error(Func &&f, Erf &&errf) ->
    typename Futurize<
      typename RetTypeOf<Func, T>::Type,
      typename ErrorFuturize<typename RetTypeOf<Erf, ErrT>::Type, T, ErrT>::ErrorType
      >::FutureType {
    using futurator = Futurize<
      typename RetTypeOf<Func, T>::Type,
      typename ErrorFuturize<typename RetTypeOf<Erf, ErrT>::Type, T, ErrT>::ErrorType
      >;
    struct Cont {
      Func f;
      Erf errf;
      Cont(Func &&f, Erf &&errf) : f(std::move(f)), errf(std::move(errf)) {}
      typename futurator::FutureType operator()(FutureState<T, ErrT> &&fs) {
	if (fs.is_error())
	  return apply_error(futurator(), std::move(errf), std::move(fs));
	else
	  return apply_value(futurator(), std::move(f), std::move(fs));
      }
    };
    return then(Cont(std::move(f), std::move(errf)));
  }

private:
  void run_one_step() {
    assert(!blocked() && !ready());
    assert(blockers.size());
    assert(blockers.front()->ready());
    std::list<Transformer::Ref> next_steps = std::move(*blockers.front()).run();
    blockers.pop_front();
    blockers.splice(blockers.begin(), std::move(next_steps));
  }

public:
  void run_until_blocked_or_ready() {
    while (!blocked() && !ready())
      run_one_step();
  }

  void wait() {
    if (blockers.empty())
      inbox.wait();
    else
      blockers.front()->wait();
    assert(!blocked());
  }

  void wait_until_ready() {
    while (blocked()) {
      wait();
      run_until_blocked_or_ready();
    }
  }
};

template <typename T, typename ErrT>
void LocalPromise<T, ErrT>::pass_state(FutureState<T, ErrT> &&f) {
  assert(valid());
  assert(target->status == (Result<T, ErrT>::Status::LOCAL));
  assert(target->local.lp == this);
  assert(!target->local.result.is_valid());
  target->local.result = std::move(f);
  target->local.lp = nullptr;
  target = nullptr;
}

template <typename T, typename ErrT>
LocalPromise<T, ErrT>::~LocalPromise() {
  if (target) {
    assert(target->status == (Result<T, ErrT>::Status::LOCAL));
    assert(target->local.lp == this);
    assert(!target->local.result.is_valid());
    target->invalidate();
    assert(!target);
  }
}

template <typename T, typename ErrT>
LocalPromise<T, ErrT>::LocalPromise(LocalPromise &&r) : target(r.target) {
  if (valid()) {
    assert(target->status == (Result<T, ErrT>::Status::LOCAL));
    assert(target->local.lp == &r);
    assert(!target->local.result.is_valid());
    target->local.lp = this;
    r.target = nullptr;
  }
};

};

template <typename T = void, typename ErrT = void>
using Promise = FutureDetail::RemotePromise<T, ErrT>;

template <typename T = void, typename ErrT = void>
using Future = FutureDetail::Future<T, ErrT>;

template <typename T, typename ErrT = int>
using FutureState = FutureDetail::FutureState<T, ErrT>;

};

#endif
