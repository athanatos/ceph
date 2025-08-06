// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

/**
 * crimson-store-bench
 *
 * This tool measures various IO patterns against the crimson FuturizedStore
 * interface.
 *
 * Usage should be:
 *
 * crimson-store-bench --store-path <path>
 *
 * where <path> is a directory containing a file block.  block should either
 * be a symlink to an actual block device or a file truncated to an appropriate
 * size if performance isn't relevant (testing or developement of this utility,
 * for instance).
 *
 * One might want to add something like the following to one's .bashrc to quickly
 * run this utility during development from build/:
 *
 * function run_store_bench {
 *   rm -rf store_bench_dir
 *   mkdir store_bench_dir
 *   touch store_bench_dir/block
 *   truncate -s 10G store_bench_dir/block
 *   ./bin/crimson-store-bench --store-path store_bench_dir $@
 * }
 */

#include <iostream>
#include <random>
#include <experimental/random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "common/ceph_time.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/coroutine.h"
#include "crimson/common/log.h"
#include "crimson/common/metrics_helpers.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace ceph;

SET_SUBSYS(osd);

using namespace std::chrono_literals;

seastar::future<bufferptr> generate_random_bp(uint64_t size)
{
  bufferptr bp(ceph::buffer::create_page_aligned(size));
  auto f = co_await seastar::open_file_dma(
    "/dev/urandom", seastar::open_flags::ro);
  static constexpr uint64_t STRIDE = 256<<10;
  for (uint64_t off = 0; off < size; off += STRIDE) {
    co_await f.dma_read(off, bp.c_str() + off, STRIDE);
  }
  co_return bp;
}

/**
 * random_write
 *
 * Performs some simple operations against store.
 * The FuturizedStore interface can be found at
 * crimson/os/futurized_store.h
 */
seastar::future<> random_write(crimson::os::FuturizedStore &global_store)
{
  LOG_PREFIX(random_write);
  /* crimson-osd's architecture partitions most resources per seastar
   * reactor.  This allows us to (mostly) avoid locking and other forms
   * of contention.  This call gets us the FuturizedStore::Shard
   * local to the reactor we are executing on. */
  auto &local_store = global_store.get_sharded_store();

  auto random_buffer = co_await generate_random_bp(16<<20);
  auto get_random_buffer = [&random_buffer](uint64_t size) {
    assert((size % CEPH_PAGE_SIZE) == 0);
    bufferptr bp(
      random_buffer,
      std::experimental::randint<uint64_t>(
        0,
        (random_buffer.length() - size) / CEPH_PAGE_SIZE) *
      CEPH_PAGE_SIZE,
      size);
    assert(bp.is_page_aligned());
    bufferlist bl;
    bl.append(bp);
    return bl;
  };

  static constexpr uint64_t PREFILL_SIZE = 128<<10;
  static constexpr uint64_t IO_SIZE = 4<<10;
  static constexpr uint64_t SIZE_PER_SHARD = 64<<20;
  static constexpr uint64_t SIZE_PER_OBJ = 4<<20;
  static constexpr uint64_t COLLS_PER_SHARD = 16;
  static constexpr uint64_t OBJ_PER_SHARD = SIZE_PER_SHARD / SIZE_PER_OBJ;
  static constexpr uint64_t OBJ_PER_COLL = OBJ_PER_SHARD / COLLS_PER_SHARD;

  auto create_hobj = [](uint64_t obj_id) {
    return ghobject_t(
      shard_id_t::NO_SHARD,
      0,     // pool id
      obj_id, // hash, normally rjenkins of name, but let's just set it to id
      "",    // namespace, empty here
      "",    // name, empty here
      0,     // snapshot
      ghobject_t::NO_GEN);
  };

  std::vector<
    std::pair<coll_t, crimson::os::CollectionRef>
    > coll_refs;
  for (uint64_t collidx = 0; collidx < COLLS_PER_SHARD; ++collidx) {
   coll_t cid(
     spg_t(pg_t(0, (seastar::this_shard_id() * COLLS_PER_SHARD) + collidx))
   );
   auto ref = co_await local_store.create_new_collection(
     cid);
   coll_refs.emplace_back(std::make_pair(cid, std::move(ref)));
  }
  auto get_coll_id = [&](uint64_t obj_id) {
    return coll_refs[obj_id % OBJ_PER_COLL].first;
  };
  auto get_coll_ref = [&](uint64_t obj_id) {
    return coll_refs[obj_id % OBJ_PER_COLL].second;
  };

  unsigned running = 0;
  std::optional<seastar::promise<>> complete;

  static constexpr unsigned IO_CONCURRENCY_PER_SHARD = 16;
  seastar::semaphore sem{IO_CONCURRENCY_PER_SHARD};
  auto submit_transaction = [&](
    crimson::os::CollectionRef &col_ref,
    ceph::os::Transaction &&t) -> seastar::future<> {
    ++running;
    co_await sem.wait(1);
    std::ignore = local_store.do_transaction(
      col_ref,
      std::move(t)
    ).finally([&] {
      --running;
      if (running == 0 && complete) {
        complete->set_value();
      }
      sem.signal(1);
    });
  };

  for (uint64_t obj_id = 0; obj_id < OBJ_PER_SHARD; ++obj_id) {
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);
    
    {
      ceph::os::Transaction t;
      t.create(coll_id, hobj);
      // actually submit the transaction and await commit
      co_await submit_transaction(coll_ref, std::move(t));
    }
    for (uint64_t off = 0; off < SIZE_PER_OBJ; off += PREFILL_SIZE) {
      ceph::os::Transaction t;
      t.write(coll_id, hobj, off, PREFILL_SIZE, get_random_buffer(PREFILL_SIZE));
      co_await submit_transaction(coll_ref, std::move(t));
    }
    INFO("wrote obj {} of {}", obj_id, OBJ_PER_SHARD);
  }

  INFO("finished populating");

  static constexpr auto TIME = 2000s;
  auto start = ceph::mono_clock::now();
  uint64_t writes_started = 0;
  while (ceph::mono_clock::now() - start < TIME) {
    auto obj_id = std::experimental::randint<uint64_t>(0, OBJ_PER_SHARD - 1);
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);

    auto offset = std::experimental::randint<uint64_t>(
      0,
      (SIZE_PER_OBJ / IO_SIZE) - 1) * IO_SIZE;
    
    ceph::os::Transaction t;
    t.write(
      coll_id,
      hobj,
      offset,
      IO_SIZE,
      get_random_buffer(IO_SIZE));
    co_await submit_transaction(coll_ref, std::move(t));
    ++writes_started;
  }

  INFO("writes_started {}", writes_started);
  for (auto &[id, ref]: coll_refs) {
    INFO("flushing {}", id);
    co_await local_store.flush(ref);
  }

  if (running > 0) {
    complete = seastar::promise<>();
    co_await complete->get_future();
  }
}

int main(int argc, char** argv)
{
  LOG_PREFIX(main);
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;
  std::string io_pattern;
  int smp;

  desc.add_options()
    ("help,h", "show help message")
    ("store-type",
     po::value<std::string>(&store_type)->default_value("seastore"),
     "set store type")
    /* store-path is a path to a directory containing a file 'block'
     * block should be a symlink to a real device for actual performance
     * testing, but may be a file for testing this utility.
     * See build/dev/osd* after starting a vstart cluster for an example
     * of what that looks like.
     */
    ("store-path", po::value<std::string>(&store_path),
     "path to store, <store-path>/block should "
     "be a symlink to the target device for bluestore or seastore")
    ("debug", po::value<bool>(&debug)->default_value(false),
     "enable debugging")
    ("smp", po::value<int>(&smp)->default_value(4),
     "number of reactors");

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
  std::cerr << "here" << std::endl;

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-store-bench";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));

  auto smp_str = std::to_string(smp);
  const char *av[] = { argv[0], "--smp", smp_str.c_str() };
  return app.run(
    sizeof(av) / sizeof(av[0]), const_cast<char **>(av),
    /* crimson-osd uses seastar as its scheduler.  We use
     * sesastar::app_template::run to start the base task for the
     * application -- this lambda.  The -> seastar::future<int> here
     * explicitely states the return type of the lambda, a future
     * which resolves to an int.  We need to do this because the
     * co_return at the end is insufficient to express the type.
     *
     * The lambda internally uses co_await/co_return and is therefore
     * a coroutine.  co_await <future> suspends execution until <future>
     * resolves.  The whole co_await expression then evaluates to the
     * contents of the future -- int for seastar::future<int>.
     *
     * What's a bit confusing is that a coroutine generally *returns*
     * at the first suspension point yielding it's return type, a
     * seastar::future<int> in this case.  This is tricky for
     * lambda-coroutines because it means that the lambda could go out
     * of scope before the coroutine actually completes, resulting in
     * captured variables (references to everything in the parent frame
     * in this case -- [&]) being free'd.  Resuming the coroutine would
     * then hit a use-after-free as soon as it tries to access any
     * of those variables.  seastar::coroutine::lambda avoids this.
     * I suggest having a look at src/seastar/include/seastar/core/coroutine.hh
     * for the implementation.
     * Note, the language guarrantees that *arguments* (whether to
     * a lambda or not) have their lifetimes extended for the duration
     * of the coroutine, so this isn't a problem for non-lambda
     * coroutines.
     */
    seastar::coroutine::lambda([&]() -> seastar::future<int> {
    if (debug) {
      seastar::global_logger_registry().set_all_loggers_level(
        seastar::log_level::debug
      );
    } else {
      seastar::global_logger_registry().set_all_loggers_level(
        seastar::log_level::info
      );
    }

    co_await crimson::common::sharded_conf().start(
      EntityName{}, std::string_view{"ceph"});
    co_await crimson::common::local_conf().start();

    {
      std::vector<const char*> cav;
      std::transform(
        std::begin(unrecognized_options),
        std::end(unrecognized_options),
        std::back_inserter(cav),
        [](auto& s) {
          return s.c_str();
        });
      co_await crimson::common::local_conf().parse_argv(
        cav);
    }

    auto store = crimson::os::FuturizedStore::create(
      store_type,
      store_path,
      crimson::common::local_conf().get_config_values()
    );

    uuid_d uuid;
    uuid.generate_random();

    co_await store->start();
    /* FuturizedStore interfaces use errorated-futures rather than bare
     * seastar futures in order to encode possible errors in the type.
     * However, this utility doesn't really need to do anything clever
     * with a failure to execute mkfs other than tell the user what
     * happened, so we simply respond uniformly to all error cases
     * using the handle_error handler.  See FuturizedStore::mkfs for
     * the actual return type and crimson/common/errorator.h for the
     * implementation of errorators.
     */
    co_await store->mkfs(uuid).handle_error(
      crimson::stateful_ec::assert_failure(
        std::format(
          "error creating empty object store type {} in {}",
          store_type,
          store_path).c_str()));

    co_await store->mount().handle_error(
      crimson::stateful_ec::assert_failure(
        std::format(
          "error mounting object store type {} in {}",
          store_type,
          store_path).c_str()));

    std::vector<seastar::future<>> completions;
    for (unsigned i = 0; i < seastar::smp::count; ++i) {
      completions.emplace_back(seastar::smp::submit_to(
        i,
        [FNAME, &store_ref=*store]() -> seastar::future<> {
          INFO("running random_write on reactor {}", seastar::this_shard_id());
          return random_write(store_ref);
        }));
    }
    for (auto &&i : completions) { co_await std::move(i); }

    JSONFormatter f(true /* pretty */);
    f.open_array_section("metrics_values");
    crimson::metrics::dump_metric_value_map(
      seastar::scollectd::get_value_map(),
      &f,
      [](const auto &) { return true; });
    f.close_section();
    f.flush(std::cout);

    co_await store->umount();
    co_await store->stop();
    co_await crimson::common::sharded_conf().stop();
    co_return 0;
  }));
}
