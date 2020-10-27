// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <linux/nbd.h>
#include <linux/fs.h>

#include "include/byteorder.h"
#include "common/ceph_time.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "test/crimson/seastar_runner.h"
#include "test/crimson/seastore/test_block.h"

#ifdef CEPH_BIG_ENDIAN
#define ntohll(a) (a)
#elif defined(CEPH_LITTLE_ENDIAN)
#define ntohll(a) swab(a)
#else
#error "Could not determine endianess"
#endif
#define htonll(a) ntohll(a)

namespace po = boost::program_options;

using namespace ceph;
using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::segment_manager::block;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

/**
 * BlockDriver
 *
 * Simple interface to enable throughput test to compare raw disk to
 * transaction_manager, etc
 */
class BlockDriver {
public:
  struct config_t {
    std::string type;
    bool mkfs = false;
    std::optional<std::string> path;
    size_t segment_size;
    size_t total_device_size;

    void populate_options(
      po::options_description &desc)
    {
      desc.add_options()
	("type",
	 po::value<std::string>()
	 ->default_value("transaction_manager")
	 ->notifier([this](auto s) { type = s; }),
	 "Backend to use, options are transaction_manager"
	)
	("segment-size",
	 po::value<size_t>()
	 ->default_value(1 << 30 /* 1GB */)
	 ->notifier([this](auto s) { segment_size = s; }),
	 "Total working set size"
	)
	("total-device-size",
	 po::value<size_t>()
	 ->default_value(4 << 10 /* 4KB */)
	 ->notifier([this](auto s) { total_device_size = s; }),
	 "Size of writes"
	)
	("device-path",
	 po::value<std::string>()
	 ->required()
	 ->notifier([this](auto s) { path = s; }),
	 "Number of writes outstanding"
	)
	("mkfs",
	 po::value<bool>()
	 ->default_value(false)
	 ->notifier([this](auto s) { mkfs = s; }),
	 "Do mkfs first"
	);
    }
  };

  virtual bufferptr get_buffer(size_t size) = 0;

  virtual seastar::future<> write(
    off_t offset,
    bufferptr ptr) = 0;

  virtual seastar::future<bufferlist> read(
    off_t offset,
    size_t size) = 0;

  virtual size_t get_size() const = 0;

  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> close() = 0;

  virtual ~BlockDriver() {}
};
using BlockDriverRef = std::unique_ptr<BlockDriver>;

BlockDriverRef get_backend(BlockDriver::config_t config);

struct request_context_t {
  uint32_t magic = 0;
  uint32_t type = 0;

  char handle[8] = {0};

  uint64_t from = 0;
  uint32_t len = 0;

  unsigned err = 0;
  std::optional<bufferptr> in_buffer;
  std::optional<bufferptr> out_buffer;

  bool check_magic() const {
    // todo
    return true;
  }

  uint32_t get_command() const {
    return type & 0xFFFF;
  }

  bool has_input_buffer() const {
    return get_command() == NBD_CMD_WRITE;
  }

  
  seastar::future<> read_request(seastar::input_stream<char> &in) {
    return in.read_exactly(sizeof(struct nbd_request)
    ).then([this, &in](auto buf) {
      auto &wire = *reinterpret_cast<const struct nbd_request *>(buf.get());
      magic = ntohl(wire.magic);
      type = ntohl(wire.type);
      memcpy(handle, wire.handle, sizeof(handle));
      from = ntohll(wire.from);
      len = ntohl(wire.len);
 
      auto next = seastar::now();
      if (has_input_buffer()) {
	next = in.read_exactly(len).then([this, &in](auto buf) {
	  in_buffer = ceph::buffer::create_page_aligned(len);
	  in_buffer->copy_in(0, len, buf.get());
	  return seastar::now();
	});
      }
    });
  }

  seastar::future<> write_reply(seastar::output_stream<char> &out) {
    using nbd_reply_t = struct nbd_reply;
    return seastar::do_with(
      nbd_reply_t(),
      [this, &out](auto &reply) {
	reply.magic = htonl(NBD_REPLY_MAGIC);
	reply.error = htonl(err);
	memcpy(reply.handle, handle, sizeof(reply.handle));
	return out.write(
	  reinterpret_cast<char*>(&reply), sizeof(reply)
	).then([this, &out] {
	  if (out_buffer) {
	    return out.write(out_buffer->c_str(), out_buffer->length());
	  } else {
	    return seastar::now();
	  }
	});
      });
  }
};

/**
 * NBDHandler
 *
 * Simple throughput test for concurrent, single threaded
 * writes to an BlockDriver.
 */
class NBDHandler {
  BlockDriver &backend;
  std::string uds_path;
public:
  struct config_t {
    std::string uds_path;

    void populate_options(
      po::options_description &desc)
    {
      desc.add_options()
	("uds-path",
	 po::value<std::string>()
	 ->default_value("")
	 ->notifier([this](auto s) {
	   uds_path = s;
	 }),
	 "/tmp/store_nbd_socket.sock"
	);
    }
  };

  NBDHandler(
    BlockDriver &backend,
    config_t config) :
    backend(backend),
    uds_path(config.uds_path)
  {}

  seastar::future<> run();
};

int main(int argc, char** argv)
{
  po::options_description desc{"Allowed options"};
  bool debug = false;
  desc.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->default_value(false),
     "enable debugging");

  po::options_description nbd_pattern_options{"NBD Pattern Options"};
  NBDHandler::config_t nbd_config;
  nbd_config.populate_options(nbd_pattern_options);
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

  seastar::app_template app;

  std::vector<char*> av{argv[0]};
  std::transform(begin(unrecognized_options),
                 end(unrecognized_options),
                 std::back_inserter(av),
                 [](auto& s) {
                   return const_cast<char*>(s.c_str());
                 });

  SeastarRunner sc;
  sc.init(av.size(), av.data());

  if (debug) {
    seastar::global_logger_registry().set_all_loggers_level(
      seastar::log_level::debug
    );
  }

  sc.run([=] {
    auto backend = get_backend(backend_config);
    return seastar::do_with(
      NBDHandler(*backend, nbd_config),
      std::move(backend),
      [](auto &nbd, auto &backend) {
	return backend->mount(
	).then([&] {
	  return nbd.run();
	}).then([&] {
	  return backend->close();
	});
      });
  });
  sc.stop();
}

class nbd_oldstyle_negotiation_t {
  ceph_le64 magic = init_le64(0x4e42444d41474943);
  ceph_le64 magic2 = init_le64(0x00420281861253);
  ceph_le64 size;
  ceph_le32 flags;
  char reserved[124] = {0};

public:
  nbd_oldstyle_negotiation_t(size_t size, unsigned flags)
    : size(init_le64(size)), flags(init_le32(flags)) {}
} __attribute__((packed));

seastar::future<> send_negotiation(
  size_t size,
  seastar::output_stream<char>& out)
{
  return seastar::do_with(
    nbd_oldstyle_negotiation_t(
      size,
      0 /* flags TODO */),
    [&out](auto &negotiation) {
      return out.write(
	reinterpret_cast<char*>(&negotiation), sizeof(negotiation));
    }).then([&out] {
      return out.flush();
    });
}

seastar::future<> handle_command(
  request_context_t &context,
  seastar::output_stream<char> &out)
{
  return ([&] {
    switch (context.get_command()) {
    case NBD_CMD_WRITE:
      logger().error("Got write");
      return seastar::now();
    case NBD_CMD_READ:
      logger().error("Got read");
      context.out_buffer = ceph::buffer::create_page_aligned(
	context.len);
      return seastar::now();
    case NBD_CMD_DISC:
      assert(0 == "disc");
      return seastar::now();
    case NBD_CMD_TRIM:
      assert(0 == "trim");
      return seastar::now();
    default:
      assert(0 == "unrecognized command");
      return seastar::now();
    }
  })().then([&] {
    return context.write_reply(out);
  });
}
  

seastar::future<> handle_commands(
  seastar::input_stream<char>& in,
  seastar::output_stream<char>& out)
{
  return seastar::keep_doing(
    [&] {
      return in.read_exactly(sizeof(struct nbd_request)
      ).then([&](auto buf) {
	auto request_ref = std::make_unique<request_context_t>();
	auto &request = *request_ref;
	return request.read_request(in
	).then([&, req=std::move(request_ref)] {
	  return handle_command(request, out);
	});
      });
    });
}

seastar::future<> NBDHandler::run()
{
  return seastar::do_with(
    seastar::engine().listen(
      seastar::socket_address{
	seastar::unix_domain_addr{uds_path}}),
    [=](auto &socket) {
      return seastar::do_until(
	[this] { /* TODO */ return false; },
	[this, &socket] {
	  return socket.accept().then([this](auto acc) {
	    return seastar::do_with(
	      std::move(acc.connection),
	      [this](auto &conn) {
		return seastar::do_with(
		  conn.input(),
		  conn.output(),
		  [&, this](auto &input, auto &output) {
		    return send_negotiation(
		      backend.get_size(),
		      output
		    ).then([&, this] {
		      return handle_commands(input, output);
		    });
		  });
	      });
	  });
	});
    });
    
  return seastar::now();
}

class TMDriver final : public BlockDriver {
  const config_t config;
  std::unique_ptr<segment_manager::block::BlockSegmentManager> segment_manager;
  std::unique_ptr<SegmentCleaner> segment_cleaner;
  std::unique_ptr<Journal> journal;
  std::unique_ptr<Cache> cache;
  LBAManagerRef lba_manager;
  std::unique_ptr<TransactionManager> tm;

public:
  TMDriver(config_t config) : config(config) {}
  ~TMDriver() final {}

  bufferptr get_buffer(size_t size) final {
    return ceph::buffer::create_page_aligned(size);
  }

  seastar::future<> write(
    off_t offset,
    bufferptr ptr) final {
    assert(offset % segment_manager->get_block_size() == 0);
    assert(ptr.length() == (size_t)segment_manager->get_block_size());
    return seastar::do_with(
      tm->create_transaction(),
      std::move(ptr),
      [this, offset](auto &t, auto &ptr) {
	return tm->dec_ref(
	  *t,
	  offset
	).safe_then([](auto){}).handle_error(
	  crimson::ct_error::enoent::discard{},
	  crimson::ct_error::pass_further_all{}
	).safe_then([=, &t, &ptr] {
	  return tm->alloc_extent<TestBlock>(
	    *t,
	    offset,
	    ptr.length());
	}).safe_then([=, &t, &ptr](auto ext) mutable {
	  assert(ext->get_laddr() == (size_t)offset);
	  assert(ext->get_bptr().length() == ptr.length());
	  ext->get_bptr().swap(ptr);
	  return tm->submit_transaction(std::move(t));
	});
      }).handle_error(
	crimson::ct_error::assert_all{}
      );
  }

  seastar::future<bufferlist> read(
    off_t offset,
    size_t size) final {
    assert(offset % segment_manager->get_block_size() == 0);
    assert(size % (size_t)segment_manager->get_block_size() == 0);
    return seastar::do_with(
      tm->create_transaction(),
      [this, offset, size](auto &t) {
	return tm->read_extents<TestBlock>(*t, offset, size
	).safe_then([=, &t](auto ext_list) mutable {
	  size_t cur = offset;
	  bufferlist bl;
	  for (auto &i: ext_list) {
	    assert(cur == i.first);
	    bl.append(i.second->get_bptr());
	    cur += i.second->get_bptr().length();
	  }
	  assert(bl.length() == size);
	  return seastar::make_ready_future<bufferlist>(std::move(bl));
	});
      }).handle_error(
	crimson::ct_error::assert_all{}
      );
  }

  void init() {
    segment_cleaner = std::make_unique<SegmentCleaner>(
      SegmentCleaner::config_t::default_from_segment_manager(
	*segment_manager),
      true);
    journal = std::make_unique<Journal>(*segment_manager);
    cache = std::make_unique<Cache>(*segment_manager);
    lba_manager = lba_manager::create_lba_manager(*segment_manager, *cache);
    tm = std::make_unique<TransactionManager>(
      *segment_manager, *segment_cleaner, *journal, *cache, *lba_manager);
    journal->set_segment_provider(&*segment_cleaner);
    segment_cleaner->set_extent_callback(&*tm);
  }

  void clear() {
    tm.reset();
    lba_manager.reset();
    cache.reset();
    journal.reset();
    segment_cleaner.reset();
  }

  size_t get_size() const final {
    return segment_manager->get_size();
  }

  seastar::future<> mkfs() {
    assert(config.path);
    segment_manager = std::make_unique<
      segment_manager::block::BlockSegmentManager
      >();
    logger().debug("mkfs");
    return segment_manager->mkfs(
      { *config.path, config.segment_size }
    ).safe_then([this] {
      logger().debug("");
      return segment_manager->mount({ *config.path });
    }).safe_then([this] {
      init();
      logger().debug("tm mkfs");
      return tm->mkfs();
    }).safe_then([this] {
      logger().debug("tm close");
      return tm->close();
    }).safe_then([this] {
      logger().debug("sm close");
      return segment_manager->close();
    }).safe_then([this] {
      clear();
      logger().debug("mkfs complete");
      return TransactionManager::mkfs_ertr::now();
    }).handle_error(
      crimson::ct_error::assert_all{}
    );
  }

  seastar::future<> mount() final {
    return (config.mkfs ? mkfs() : seastar::now()
    ).then([this] {
      segment_manager = std::make_unique<
	segment_manager::block::BlockSegmentManager
	>();
      return segment_manager->mount({ *config.path });
    }).safe_then([this] {
      init();
      return tm->mount();
    }).handle_error(
      crimson::ct_error::assert_all{}
    );
  };

  seastar::future<> close() final {
    return segment_manager->close(
    ).safe_then([this] {
      return tm->close();
    }).safe_then([this] {
      clear();
      return seastar::now();
    }).handle_error(
      crimson::ct_error::assert_all{}
    );
  }
};

BlockDriverRef get_backend(BlockDriver::config_t config)
{
  if (config.type == "transaction_manager") {
    return std::make_unique<TMDriver>(config);
  } else {
    assert(0 == "invalid option");
  }
  return BlockDriverRef();
}
