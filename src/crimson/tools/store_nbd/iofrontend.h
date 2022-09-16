// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/core/future.hh>

#include "block_driver.h"

/**
 * IOFrontend
 *
 * Interface for IO interface to client
 */
class IOFrontend {
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

  virtual void run() = 0;
  virtual seastar::future<> stop() = 0;

  using ref_t = std::unique_ptr<IOFrontend>;
  static ref_t get_io_frontend(BlockDriver &bdriver, config_t config);
};
