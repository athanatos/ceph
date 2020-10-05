// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"

#include "test/crimson/seastore/test_block.h"


class lba_throughput_test {
public:
};

int main(int argc, char** argv)
{
#if 0
  namespace po = boost::program_options;
  po::options_description desc{"Allowed options"};
  desc.add_options()
    ("help,h", "show help message")
    ("device", po::value<std::string>(),
     "path to device")
    ("segment_size", po::value<uint32_t>()->default_value(128<<20 /* 128M */),
     "size to use for segments");

  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
    po::notify(vm);
    unrecognized_options =
      po::collect_unrecognized(parsed.options, po::include_positional);
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
  std::vector<const char*> args(argv, argv + argc);

  auto count = vm["count"].as<unsigned>();
  seastar::app_template app;

  SeastarContext sc;
  auto job = sc.with_seastar([&] {
    auto fut = seastar::alien::submit_to(0, [addr, role, count] {
      return seastar::now();
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
