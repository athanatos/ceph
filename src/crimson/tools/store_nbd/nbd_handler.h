// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "block_driver.h"

/**
 * NBDHandler
 *
 * Simple throughput test for concurrent, single threaded
 * writes to an BlockDriver.
 */
class NBDHandler {
  BlockDriver &backend;
  std::string uds_path;
  std::optional<seastar::server_socket> server_socket;
  std::optional<seastar::connected_socket> connected_socket;
  seastar::gate gate;
public:
  struct config_t {
    std::string uds_path;

    void populate_options(
      boost::program_options::options_description &desc)
    {
      desc.add_options()
	("uds-path",
	 boost::program_options::value<std::string>()
	 ->default_value("/tmp/store_nbd_socket.sock")
	 ->notifier([this](auto s) {
	   uds_path = s;
	 }),
	 "Path to domain socket for nbd"
	);
    }
  };

  NBDHandler(
    BlockDriver &backend,
    config_t config) :
    backend(backend),
    uds_path(config.uds_path)
  {}

  void run();
  seastar::future<> stop();
};

