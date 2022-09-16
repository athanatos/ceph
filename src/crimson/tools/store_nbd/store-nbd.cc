// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

/**
 * crimson-store-nbd
 *
 * This tool exposes crimson object store internals as an nbd server
 * for use with fio in basic benchmarking.
 *
 * Example usage:
 *
 *  $ ./bin/crimson-store-nbd --device-path /dev/nvme1n1 -c 1 --mkfs true --uds-path /tmp/store_nbd_socket.sock
 *
 *  $ cat nbd.fio
 *  [global]
 *  ioengine=nbd
 *  uri=nbd+unix:///?socket=/tmp/store_nbd_socket.sock
 *  rw=randrw
 *  time_based
 *  runtime=120
 *  group_reporting
 *  iodepth=1
 *  size=500G
 *
 *  [job0]
 *  offset=0
 *
 *  $ fio nbd.fio
 */

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"

#include "iofrontend.h"
#include "block_driver.h"

namespace po = boost::program_options;

using namespace ceph;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

int main(int argc, char** argv)
{
  po::options_description desc{"Allowed options"};
  bool debug = false;
  desc.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->default_value(false),
     "enable debugging");

  po::options_description nbd_pattern_options{"NBD Pattern Options"};
  IOFrontend::config_t frontend_config;
  frontend_config.populate_options(nbd_pattern_options);
  desc.add(nbd_pattern_options);

  po::options_description backend_pattern_options{"Backend Options"};
  BlockDriver::config_t backend_config;
  backend_config.populate_options(backend_pattern_options);
  desc.add(backend_pattern_options);

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
 }  catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
  std::vector<const char*> args(argv, argv + argc);

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-store-nbd";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));

  std::vector<char*> av{argv[0]};
  std::transform(begin(unrecognized_options),
                 end(unrecognized_options),
                 std::back_inserter(av),
                 [](auto& s) {
                   return const_cast<char*>(s.c_str());
                 });
  return app.run(av.size(), av.data(), [&] {
    if (debug) {
      seastar::global_logger_registry().set_all_loggers_level(
        seastar::log_level::debug
      );
    }
    return seastar::async([&] {
      seastar_apps_lib::stop_signal should_stop;
      crimson::common::sharded_conf()
        .start(EntityName{}, std::string_view{"ceph"}).get();
      auto stop_conf = seastar::defer([] {
        crimson::common::sharded_conf().stop().get();
      });

      auto backend = get_backend(backend_config);
      IOFrontend::ref_t frontend = IOFrontend::get_io_frontend(
	*backend, frontend_config);
      backend->mount().get();
      auto close_backend = seastar::defer([&] {
        backend->close().get();
      });

      logger().debug("Running nbd server...");
      frontend->run();
      auto stop_nbd = seastar::defer([&] {
        frontend->stop().get();
      });
      should_stop.wait().get();
      return 0;
    });
  });
}

