// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

seastar::future<> spdk_reactor_start();
seastar::future<> spdk_reactor_stop();

class seastar_spdk_thread_t {
  spdk_thread *thread = nullptr;
  const char *name = nullptr;

  static void _run_msg(void*);
  using msg_func_t = std::function<void()>;
  void send_msg(msg_func_t *f);
public:
  seastar_spdk_thread_t(const char *name) : name(name) {}
  ~seastar_spdk_thread_t();
  void start();
  seastar::future<> exit();

  template <typename F, typename Ret>
  seastar::future<Ret> send_msg_with_promise(F &&l) {
    seastar::promise<Ret> p;
    msg_func_t f([l=std::move(l), &p] {
      std::invoke(l, &p);
    });
    send_msg(&f);
    co_return co_await p.get_future();
  }

  template <typename F>
  seastar::future<> invoke_on(F &&f) {
    return send_msg_with_promise([f=std::move(f)](auto *p) {
      std::invoke(f);
      p->set_value();
    });
  }
};
