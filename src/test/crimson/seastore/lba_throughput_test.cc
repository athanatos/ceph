// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "common/ceph_time.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "test/crimson/seastar_runner.h"
#include "test/crimson/seastore/test_block.h"

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
 * LBAThroughputBackend
 *
 * Simple interface to enable throughput test to compare raw disk to
 * transaction_manager, etc
 */
class LBAThroughputBackend {
public:
  struct config_t {
    std::string type;
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

  virtual seastar::future<> mkfs() = 0;
  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> close() = 0;

  virtual ~LBAThroughputBackend() {}
};
using LBAThroughputBackendRef = std::unique_ptr<LBAThroughputBackend>;

LBAThroughputBackendRef get_backend(LBAThroughputBackend::config_t config);

/**
 * LBAThroughputTest
 *
 * Simple throughput test for concurrent, single threaded
 * writes to an LBAThroughputBackend.
 */
class LBAThroughputTest {
  LBAThroughputBackend &backend;
  const size_t total_size; 
  const size_t write_size; 
  const size_t queue_depth; 
public:
  struct config_t {
    size_t total_size = 0;
    size_t write_size = 0;
    size_t queue_depth = 0;

    timespan duration;

    void populate_options(
      po::options_description &desc)
    {
      desc.add_options()
	("total-size",
	 po::value<size_t>()
	 ->default_value(1 << 30 /* 1GB */)
	 ->notifier([this](auto s) { total_size = s; }),
	 "Total working set size"
	)
	("write-size",
	 po::value<size_t>()
	 ->default_value(4 << 10 /* 4KB */)
	 ->notifier([this](auto s) { write_size = s; }),
	 "Size of writes"
	)
	("queue-depth",
	 po::value<size_t>()
	 ->default_value(1)
	 ->notifier([this](auto s) { queue_depth = s; }),
	 "Number of writes outstanding"
	);
    }
  };

  LBAThroughputTest(
    LBAThroughputBackend &backend,
    config_t config) :
    backend(backend),
    total_size(config.total_size),
    write_size(config.write_size),
    queue_depth(config.queue_depth)
  {}

  seastar::future<> run();
};

int main(int argc, char** argv)
{
  po::options_description desc{"Allowed options"};
  desc.add_options()
    ("help,h", "show help message")
    ("device", po::value<std::string>(),
     "path to device")
    ("backend", po::value<std::string>()->default_value("transaction_manager"),
     "Backend to use for workload")
    ("segment_size", po::value<uint32_t>()->default_value(128<<20 /* 128M */),
     "size to use for segments")
    ("debug", po::value<bool>()->default_value(false),
     "enable debugging");

  po::options_description io_pattern_options{"IO Pattern Options"};
  LBAThroughputTest::config_t io_config;
  io_config.populate_options(io_pattern_options);
  desc.add(io_pattern_options);

  po::options_description backend_pattern_options{"Backend Options"};
  LBAThroughputBackend::config_t backend_config;
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

  if (vm.count("debug")) {
    seastar::global_logger_registry().set_all_loggers_level(
      seastar::log_level::debug
    );
  }

  sc.run([=] {
    auto backend = get_backend(backend_config);
    auto tester = get_backend(backend_config);
    return seastar::do_with(
      LBAThroughputTest{*backend, io_config},
      std::move(backend),
      [](auto &tester, auto &backend) {
	return backend->mkfs(
	).then([&] {
	  return backend->mount();
	}).then([&] {
	  return tester.run();
	}).then([&] {
	  return backend->close();
	});
      });
  });
  sc.stop();
}

seastar::future<> LBAThroughputTest::run()
{
  return seastar::now();
}

class TMBackend final : public LBAThroughputBackend {
  const config_t config;
  std::unique_ptr<segment_manager::block::BlockSegmentManager> segment_manager;
  std::unique_ptr<SegmentCleaner> segment_cleaner;
  std::unique_ptr<Journal> journal;
  std::unique_ptr<Cache> cache;
  LBAManagerRef lba_manager;
  std::unique_ptr<TransactionManager> tm;

public:
  TMBackend(config_t config) : config(config) {}
  ~TMBackend() final {}

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

  seastar::future<> mkfs() final {
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
    segment_manager = std::make_unique<
      segment_manager::block::BlockSegmentManager
      >();
    return segment_manager->mount({ *config.path }
    ).safe_then([this] {
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

LBAThroughputBackendRef get_backend(LBAThroughputBackend::config_t config)
{
  if (config.type == "transaction_manager") {
    return std::make_unique<TMBackend>(config);
  } else {
    assert(0 == "invalid option");
  }
  return LBAThroughputBackendRef();
}
