// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <stdio.h>
#include <signal.h>
#include <thread>

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

#include "gtest/gtest.h"

struct seastar_gtest_env_t {
  seastar::app_template app;
  seastar::file_desc begin_fd;
  seastar::readable_eventfd on_end;

  std::thread thread;

  seastar_gtest_env_t();
  ~seastar_gtest_env_t();

  void run();
};
    
struct seastar_test_suite_t : public ::testing::Test {
  static seastar_gtest_env_t seastar_env;

  void SetUp() final {
#if 0
    seastar::app_template app;
    SeastarContext sc;
    auto job = sc.with_seastar([&] {
      auto fut = seastar::alien::submit_to(0, [addr, role, count] {
	return seastar_echo(addr, role, count);
      });
      fut.wait();
    });
    std::vector<char*> av{argv[0]};
    std::transform(begin(unrecognized_options),
		   end(unrecognized_options),
		   std::back_inserter(av),
		   [](auto& s) {
		     return const_cast<char*>(s.c_str());
		   });
    sc.run(app, av.size(), av.data());
    job.join();
#endif
  }

  void TearDown() final {
  }
};
