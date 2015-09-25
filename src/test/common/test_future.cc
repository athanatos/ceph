// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <string>

#include "common/Future.h"

#include "gtest/gtest.h"

using namespace Ceph;

template<typename T, typename ErrT>
struct PromiseResultTest {
  static void run(T test_with, ErrT err_with) {
    using namespace Ceph::FutureDetail;
    {
      Result<T, ErrT> res;
      ASSERT_FALSE(res.valid());
      res = std::move(res);
      {
	auto p = res.get_local_promise();
	p = std::move(p);
	ASSERT_TRUE(res.valid());
	res = std::move(res);
      }
      ASSERT_FALSE(res.valid());
    }
    {
      LocalPromise<T, ErrT> p;
      p = std::move(p);
      ASSERT_FALSE(p.valid());
      {
	Result<T, ErrT> res;
	ASSERT_FALSE(res.valid());
	p = res.get_local_promise();
	ASSERT_TRUE(res.valid());
	ASSERT_TRUE(p.valid());
      }
      ASSERT_FALSE(p.valid());
    }
    {
      Result<T, ErrT> res;
      ASSERT_FALSE(res.valid());
      res = std::move(res);
      {
	auto p = res.get_remote_promise();
	p = std::move(p);
	ASSERT_TRUE(res.valid());
	res = std::move(res);
      }
      ASSERT_FALSE(res.valid());
    }
    {
      RemotePromise<T, ErrT> p;
      p = std::move(p);
      ASSERT_FALSE(p.valid());
      {
	Result<T, ErrT> res;
	ASSERT_FALSE(res.valid());
	p = res.get_remote_promise();
	ASSERT_TRUE(res.valid());
	ASSERT_TRUE(p.valid());
      }
      ASSERT_FALSE(p.valid());
    }
    {
      Result<T, ErrT> res;
      ASSERT_FALSE(res.valid());
      auto p = res.get_local_promise();
      ASSERT_TRUE(p.valid());
      ASSERT_TRUE(res.valid());
      ASSERT_FALSE(res.ready());
      p.fulfill(T(test_with));
      ASSERT_FALSE(p.valid());
      ASSERT_TRUE(res.valid());
      ASSERT_TRUE(res.ready());
      ASSERT_FALSE(res.is_error());
      ASSERT_EQ(test_with, res.get().get_value());
    }
    {
      Result<T, ErrT> res;
      ASSERT_FALSE(res.valid());
      auto p = res.get_local_promise();
      ASSERT_TRUE(p.valid());
      ASSERT_TRUE(res.valid());
      ASSERT_FALSE(res.ready());
      p.error(ErrT(err_with));
      ASSERT_FALSE(p.valid());
      ASSERT_TRUE(res.valid());
      ASSERT_TRUE(res.ready());
      ASSERT_TRUE(res.is_error());
      ASSERT_EQ(err_with, res.get().get_error());
    }
  }
};

TEST(Future, local_result_promise)
{
  PromiseResultTest<int, int>::run(0, 0);
  PromiseResultTest<std::string, int>::run("asdf", 0);
  PromiseResultTest<int, std::string>::run(0, "asdf");
  PromiseResultTest<std::string, std::string>::run("asdf", "asdf");
}

TEST(Future, basic)
{
  {
    Future<int, int> fut;
    Promise<int, int> p(fut.get_promise());
    ASSERT_FALSE(fut.ready());
    p.fulfill(1);
    ASSERT_TRUE(fut.ready());
    ASSERT_TRUE(fut.valid());
    ASSERT_FALSE(p.valid());
    ASSERT_EQ(fut.get_value(), 1);
    ASSERT_FALSE(fut.valid());
    ASSERT_FALSE(p.valid());
  }

  {
    Future<int, int> fut;
    Promise<int, int> p(fut.get_promise());
    Future<int, int> fut2 = fut.then_ignore_error([](int in) { return in + 1; });
    ASSERT_FALSE(fut.valid());
    ASSERT_TRUE(fut2.valid());
    ASSERT_TRUE(p.valid());
    ASSERT_TRUE(fut2.blocked());
    p.fulfill(1);
    ASSERT_FALSE(fut2.blocked());
    ASSERT_FALSE(fut2.ready());
    fut2.run_until_blocked_or_ready();
    ASSERT_FALSE(fut2.blocked());
    ASSERT_TRUE(fut2.ready());
    ASSERT_FALSE(p.valid());
    ASSERT_EQ(fut2.get_value(), 2);
    ASSERT_FALSE(fut2.valid());
  }

  {
    Future<int, int> fut;
    Promise<int, int> p(fut.get_promise());
    Future<int, int> fut2 = fut.then_ignore_error([](int in) { return in + 10; });
    ASSERT_FALSE(fut.valid());
    ASSERT_TRUE(fut2.valid());

    Promise<int, int> p2;
    Promise<int, int> *_p2 = &p2;
    Future<int, std::string> fut3 = fut2.then(
      [=](FutureState<int, int> &&_in) mutable {
	int in = _in.get_value();
	Future<int, int> ret;
	*_p2 = std::move(ret.get_promise());
	Future<int, std::string> ret2 = ret.then_with_error(
	  [=](int in2) {
	    return in + in2;
	  },
	  [=](int err) {
	    return std::string();
	  });
	return ret2;
      });
    ASSERT_FALSE(fut2.valid());
    ASSERT_TRUE(fut3.valid());

    ASSERT_TRUE(p.valid());
    ASSERT_FALSE(p2.valid());

    ASSERT_TRUE(fut3.blocked());
    p.fulfill(1);
    ASSERT_FALSE(fut3.blocked());
    ASSERT_FALSE(fut3.ready());
    fut3.run_until_blocked_or_ready();
    ASSERT_TRUE(fut3.blocked());
    ASSERT_FALSE(fut3.ready());
    ASSERT_FALSE(p.valid());
    ASSERT_TRUE(p2.valid());
    p2.fulfill(100);
    ASSERT_FALSE(p2.valid());
    ASSERT_FALSE(fut3.blocked());

    fut3.run_until_blocked_or_ready();
    ASSERT_FALSE(fut3.blocked());
    ASSERT_TRUE(fut3.ready());

    ASSERT_EQ(fut3.get_value(), 111);
    ASSERT_FALSE(fut3.valid());
  }

  {
    Future<int, int> fut;
    Promise<int, int> p(fut.get_promise());
    Future<std::string, int> fut2 = fut.then_ignore_error([](int in) {
	std::stringstream ss;
	ss << (in + 10);
	return ss.str();
      });
    ASSERT_FALSE(fut.valid());
    ASSERT_TRUE(fut2.valid());

    Promise<std::string, int> p2;
    Promise<std::string, int> *_p2 = &p2;
    Future<std::string, std::string> fut3 = fut2.then(
      [=](FutureState<std::string, int> &&_in) mutable {
	std::string in = _in.get_value();
	Future<std::string, int> ret;
	*_p2 = std::move(ret.get_promise());
	Future<std::string, std::string> ret2 = ret.then_with_error(
	  [=](std::string &&in2) {
	    return in + in2;
	  },
	  [=](int err) {
	    std::stringstream ss;
	    ss << err;
	    return ss.str();
	  }).then_with_error(
	    [=](std::string &&in2) {
	      return FutureState<std::string, std::string>::make_value(
		std::move(in2));
	    },
	    [=](std::string &&in2) {
	      return FutureState<std::string, std::string>::make_error(
		std::move(in2));
	    });
	return ret2;
      });
    ASSERT_FALSE(fut2.valid());
    ASSERT_TRUE(fut3.valid());

    ASSERT_TRUE(p.valid());
    ASSERT_FALSE(p2.valid());

    ASSERT_TRUE(fut3.blocked());
    p.fulfill(1);
    ASSERT_FALSE(fut3.blocked());
    ASSERT_FALSE(fut3.ready());
    fut3.run_until_blocked_or_ready();
    ASSERT_TRUE(fut3.blocked());
    ASSERT_FALSE(fut3.ready());
    ASSERT_FALSE(p.valid());
    ASSERT_TRUE(p2.valid());
    p2.error(-2);
    ASSERT_FALSE(p2.valid());
    ASSERT_FALSE(fut3.blocked());

    fut3.run_until_blocked_or_ready();
    ASSERT_FALSE(fut3.blocked());
    ASSERT_TRUE(fut3.ready());

    ASSERT_EQ(fut3.get_error(), "-2");
    ASSERT_FALSE(fut3.valid());
  }
}

template<typename T, typename ErrT, typename T2, typename ErrT2>
struct TestFromTo {
  const T t;
  const ErrT et;
  const T2 t2;
  const ErrT2 et2;
  TestFromTo(
    T t,
    ErrT et,
    T2 t2,
    ErrT2 et2)
    : t(t), et(et), t2(t2), et2(et2) {}

  void test_then() const {
    auto f1 = Future<T, ErrT>();
    auto p1 = f1.get_promise();
    auto f2 = f1.then(
      [=](FutureState<T, ErrT> &&){
	return t2;
      });
    auto f3 = f2.then(
      [=](FutureState<T2, ErrT> &&) {
	return FutureState<T2, ErrT2>::make_error(et2);
      });
    auto f4 = f3.then(
      [=](FutureState<T2, ErrT2> &&) {
	return FutureState<T2, void>::make_error();
      });
    auto f5 = f4.then(
      [=](FutureState<T2, void> &&) {
	return;
      });
    auto f6 = f5.then(
      [=](FutureState<void, void> &&) {
	return t;
      });
    ASSERT_TRUE(f6.valid());
    ASSERT_FALSE(f6.ready());
    ASSERT_FALSE(f6.ready());
    ASSERT_TRUE(f6.blocked());
    p1.error(et);
    ASSERT_FALSE(f6.blocked());
    f6.run_until_blocked_or_ready();
    ASSERT_TRUE(f6.valid());
    ASSERT_TRUE(f6.ready());
    ASSERT_FALSE(f6.blocked());
    ASSERT_FALSE(f6.is_error());
  }
};

TEST(Future, types)
{
  using std::string;
  TestFromTo<int, int, string, string> t(0, 1, "0", "1");
  t.test_then();
}
