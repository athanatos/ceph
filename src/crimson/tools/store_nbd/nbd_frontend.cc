// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "nbd_frontend.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


class nbd_oldstyle_negotiation_t {
  uint64_t magic = seastar::cpu_to_be(0x4e42444d41474943); // "NBDMAGIC"
  uint64_t magic2 = seastar::cpu_to_be(0x00420281861253);  // "IHAVEOPT"
  uint64_t size = 0;
  uint32_t flags = seastar::cpu_to_be(0);
  char reserved[124] = {0};

public:
  nbd_oldstyle_negotiation_t(uint64_t size, uint32_t flags)
    : size(seastar::cpu_to_be(size)), flags(seastar::cpu_to_be(flags)) {}
} __attribute__((packed));

seastar::future<> send_negotiation(
  size_t size,
  seastar::output_stream<char>& out)
{
  seastar::temporary_buffer<char> buf{sizeof(nbd_oldstyle_negotiation_t)};
  new (buf.get_write()) nbd_oldstyle_negotiation_t(size, 1);
  return out.write(std::move(buf)
  ).then([&out] {
    return out.flush();
  });
}

seastar::future<> handle_command(
  BlockDriver &backend,
  request_context_t::ref request_ref,
  RequestWriter &out)
{
  auto &request = *request_ref;
  logger().debug("got command {}", request.get_command());
  return ([&] {
    switch (request.get_command()) {
    case NBD_CMD_WRITE:
      return backend.write(
	request.from,
	*request.in_buffer);
    case NBD_CMD_READ:
      return backend.read(
	request.from,
	request.len).then([&] (auto buffer) {
	  logger().debug("read returned buffer len {}", buffer.length());
	  request.out_buffer = buffer;
	});
    case NBD_CMD_DISC:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    case NBD_CMD_TRIM:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    default:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    }
  })().then([&, request_ref=std::move(request_ref)]() mutable {
    logger().debug("handle_command complete");
    return out.complete(std::move(request_ref));
  });
}


seastar::future<> handle_commands(
  BlockDriver &backend,
  seastar::input_stream<char>& in,
  RequestWriter &out)
{
  logger().debug("handle_commands");
  return seastar::keep_doing([&] {
    logger().debug("waiting for command");
    auto request_ref = request_context_t::make_ref();
    auto &request = *request_ref;
    return request.read_request(in).then(
      [&, request_ref=std::move(request_ref)]() mutable {
      // keep running in background
      (void)seastar::try_with_gate(out.gate,
        [&backend, &out, request_ref=std::move(request_ref)]() mutable {
        return handle_command(backend, std::move(request_ref), out);
      });
      logger().debug("handle_commands after fork");
    });
  }).handle_exception_type([](const seastar::gate_closed_exception&) {});
}

void NBDFrontend::run()
{
  logger().debug("About to listen on {}", uds_path);
  server_socket = seastar::engine().listen(
      seastar::socket_address{
      seastar::unix_domain_addr{uds_path}});

  // keep running in background
  (void)seastar::keep_doing([this] {
    return seastar::try_with_gate(gate, [this] {
      return server_socket->accept().then([this](auto acc) {
        logger().debug("Accepted");
        connected_socket = std::move(acc.connection);
        return seastar::do_with(
          connected_socket->input(),
          RequestWriter{connected_socket->output()},
          [&, this](auto &input, auto &output) {
            return send_negotiation(
              backend.get_size(),
              output.stream
            ).then([&, this] {
              return handle_commands(backend, input, output);
            }).finally([&] {
              std::cout << "closing input and output" << std::endl;
              return seastar::when_all(input.close(),
                                       output.close());
            }).discard_result().handle_exception([](auto e) {
              logger().error("NBDFrontend::run saw exception {}", e);
            });
          });
      }).handle_exception_type([] (const std::system_error &e) {
        // an ECONNABORTED is expected when we are being stopped.
        if (e.code() != std::errc::connection_aborted) {
          logger().error("accept failed: {}", e);
        }
      });
    });
  }).handle_exception_type([](const seastar::gate_closed_exception&) {});
}

seastar::future<> NBDFrontend::stop()
{
  if (server_socket) {
    server_socket->abort_accept();
  }
  if (connected_socket) {
    connected_socket->shutdown_input();
    connected_socket->shutdown_output();
  }
  return gate.close().then([this] {
    if (!server_socket.has_value()) {
      return seastar::now();
    }
    return seastar::remove_file(uds_path);
  });
}
