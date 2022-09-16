// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdint>

#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <linux/nbd.h>
#include <linux/fs.h>

#include "crimson/common/log.h"

#include "iofrontend.h"

struct request_context_t {
  uint32_t magic = 0;
  uint32_t type = 0;

  char handle[8] = {0};

  uint64_t from = 0;
  uint32_t len = 0;

  unsigned err = 0;
  std::optional<bufferptr> in_buffer;
  std::optional<bufferlist> out_buffer;

  using ref = std::unique_ptr<request_context_t>;
  static ref make_ref() {
    return std::make_unique<request_context_t>();
  }

  bool check_magic() const {
    auto ret = magic == NBD_REQUEST_MAGIC;
    if (!ret) {
      crimson::get_logger(ceph_subsys_test).error(
	"Invalid magic {} should be {}",
	magic,
	NBD_REQUEST_MAGIC);
    }
    return ret;
  }

  uint32_t get_command() const {
    return type & 0xff;
  }

  bool has_input_buffer() const {
    return get_command() == NBD_CMD_WRITE;
  }

  seastar::future<> read_request(seastar::input_stream<char> &in) {
    return in.read_exactly(sizeof(struct nbd_request)
    ).then([this, &in](auto buf) {
      if (buf.size() < sizeof(struct nbd_request)) {
	throw std::system_error(
	  std::make_error_code(
	    std::errc::connection_reset));
      }
      auto p = buf.get();
      magic = seastar::consume_be<uint32_t>(p);
      type = seastar::consume_be<uint32_t>(p);
      memcpy(handle, p, sizeof(handle));
      p += sizeof(handle);
      from = seastar::consume_be<uint64_t>(p);
      len = seastar::consume_be<uint32_t>(p);
      crimson::get_logger(ceph_subsys_test).debug(
        "Got request, magic {}, type {}, from {}, len {}",
	magic, type, from, len);

      if (!check_magic()) {
       throw std::system_error(
	 std::make_error_code(
	   std::errc::invalid_argument));
      }

      if (has_input_buffer()) {
	return in.read_exactly(len).then([this](auto buf) {
	  in_buffer = ceph::buffer::create_page_aligned(len);
	  in_buffer->copy_in(0, len, buf.get());
	  return seastar::now();
	});
      } else {
	return seastar::now();
      }
    });
  }

  seastar::future<> write_reply(seastar::output_stream<char> &out) {
    seastar::temporary_buffer<char> buffer{sizeof(struct nbd_reply)};
    auto p = buffer.get_write();
    seastar::produce_be<uint32_t>(p, NBD_REPLY_MAGIC);
    seastar::produce_be<uint32_t>(p, err);
    crimson::get_logger(ceph_subsys_test).debug(
      "write_reply writing err {}", err);
    memcpy(p, handle, sizeof(handle));
    return out.write(std::move(buffer)).then([this, &out] {
      if (out_buffer) {
        return seastar::do_for_each(
          out_buffer->mut_buffers(),
          [&out](bufferptr &ptr) {
	    crimson::get_logger(ceph_subsys_test).debug(
	      "write_reply writing {}", ptr.length());
            return out.write(
	      seastar::temporary_buffer<char>(
		ptr.c_str(),
		ptr.length(),
		seastar::make_deleter([ptr](){}))
	    );
          });
      } else {
        return seastar::now();
      }
    }).then([&out] {
      return out.flush();
    });
  }
};

struct RequestWriter {
  seastar::rwlock lock;
  seastar::output_stream<char> stream;
  seastar::gate gate;

  RequestWriter(
    seastar::output_stream<char> &&stream) : stream(std::move(stream)) {}
  RequestWriter(RequestWriter &&) = default;

  seastar::future<> complete(request_context_t::ref &&req) {
    auto &request = *req;
    return lock.write_lock(
    ).then([&request, this] {
      return request.write_reply(stream);
    }).finally([&, this, req=std::move(req)] {
      lock.write_unlock();
      crimson::get_logger(ceph_subsys_test).debug(
	"complete");
      return seastar::now();
    });
  }

  seastar::future<> close() {
    return gate.close().then([this] {
      return stream.close();
    });
  }
};

/**
 * NBDFrontend
 *
 * Simple throughput test for concurrent, single threaded
 * writes to an BlockDriver.
 */
class NBDFrontend : public IOFrontend {
  BlockDriver &backend;
  std::string uds_path;
  std::optional<seastar::server_socket> server_socket;
  std::optional<seastar::connected_socket> connected_socket;
  seastar::gate gate;
public:
  NBDFrontend(
    BlockDriver &backend,
    config_t config) :
    backend(backend),
    uds_path(config.uds_path)
  {}

  void run();
  seastar::future<> stop();
};
