// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest_seastar.h"

seastar_gtest_env_t seastar_test_suite_t::seastar_env;

seastar_gtest_env_t::seastar_gtest_env_t() :
  begin_fd{seastar::file_desc::eventfd(0, 0)},
  thread{[this] { run(); }}
{
  eventfd_t result = 0;
  if (int r = ::eventfd_read(begin_fd.get(), &result); r < 0) {
    std::cerr << "unable to eventfd_read():" << errno << std::endl;
    throw std::runtime_error("Cannot start seastar");
  }
}

seastar_gtest_env_t::~seastar_gtest_env_t()
{
  on_end.write_side().signal(1);
  thread.join();
}

void seastar_gtest_env_t::run()
{
  char** argv = nullptr;
  app.run(0, argv, [this] {
    return seastar::now().then([this] {
      ::eventfd_write(begin_fd.get(), 1);
	return seastar::now();
    }).then([this] {
      return on_end.wait().then([](size_t){});
    }).handle_exception([](auto ep) {
      std::cerr << "Error: " << ep << std::endl;
    }).finally([] {
      seastar::engine().exit(0);
    });
  });
}

int main(int argc, char **argv)
{

  std::vector<const char*> args(argv, argv + argc);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  return ret;
}
