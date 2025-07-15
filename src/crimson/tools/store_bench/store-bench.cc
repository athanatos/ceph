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
 * One might want to add something like the following to one's .bashrc to
 * quickly run this utility during development from build/:
 *
 */

#include <random>
#include <vector>

#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "crimson/common/config_proxy.h"
#include "crimson/common/coroutine.h"
#include "crimson/common/log.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace ceph;

SET_SUBSYS(osd);

/**
* Creates a unique object
*/
 ghobject_t create_hobj (unsigned id) {
  return ghobject_t(
      shard_id_t::NO_SHARD, seastar::this_shard_id(),
      id, // hash, normally rjenkins of name, but let's just set it to id
      "", // namespace, empty here
      "", // name, empty here
      0,  // snapshot
      ghobject_t::NO_GEN);
  };

  /**
   * This function creates a collection with pool id as obj_id
   * The function is called when we create an object
   * the goal is to have each object belong to a separate collection
   */
  coll_t make_cid (int obj_id) { 
    return coll_t(spg_t(pg_t(obj_id, 0))); 
  };

  /**
   * The struct stores the results for a single io
   * The number of operations is incrmented by 1 for write+remove
   * latency per_io is the sum of times it takes per opertaion
   */
  struct results_t {
    int num_operations;
    int tot_latency_per_io;
  };

  

/**
 * This function adds and removes log entries to a log object
 * It returns throughput(number of operations/milli sec)
 * Also returns average latency (time/operation)
 * This function is split into 3 steps:
 * (a)pre filling the logs to steady state
 * (b) writing and removing logs ,this is considered 1 I/O
 * (c) doing N I/o's concurrently on 1 thread
 */
seastar::future<> pg_log_workload(crimson::os::FuturizedStore &global_store,
                                  int num_logs, int num_concurrent_io,
                                  int duration, int log_size, int log_length) {
  LOG_PREFIX(pg_log_workload);
  auto &local_store = global_store.get_sharded_store();

  std::map<int, coll_t> collection_id;

  /**
   * This method returns a future with pre filled logs
   * It creates num_log number of log objects
   * Each log object has log_length number of entries
   * Each entry is a key value pair, with the value having size log_size
   */

  auto pre_fill_logs = [&]() -> seastar::future<> {
    for (int i = 0; i < num_logs; ++i) {
      auto obj_i = create_hobj(i);
      auto coll_id = make_cid(i);
      collection_id[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      std::map<std::string, bufferlist> data;
      for (int j = 0; j < log_length; ++j) {
        std::string key = std::to_string(j);
        bufferlist bl_value;
        bl_value.append_zero(log_size);
        data[key] = bl_value;
      }
      ceph::os::Transaction txn;
      txn.create(coll_id, obj_i);
      txn.omap_setkeys(coll_id, obj_i, data);
      co_await local_store.do_transaction(coll_ref, std::move(txn));
    }
    co_return;
  };

  std::vector<int> first_key_per_log(num_logs,
                                     0); // first key in each log object
  std::vector<int> last_key_per_log(num_logs,
                                    log_length); // last key in each log object

  /**
   * This method returns a future of type struct results_t
   * In this function we choose a random log object to write to and remove keys
   * from We add and remove keys sequentially, add keys to the end, remove from
   * the front Total latency per io is calculated as the sum of the time it
   * takes to add and remove each key
   */
  auto add_remove_entry = [&]() -> seastar::future<results_t> {
    int num_ops = 0;
    int tot_latency = 0;
    auto start = seastar::lowres_clock::now();

    while (seastar::lowres_clock::now() - start <=
           std::chrono::milliseconds(duration)) {
      int obj_num = std::rand() % num_logs;
      auto object = create_hobj(obj_num);
      auto coll_id = collection_id[obj_num];
      auto coll_ref = co_await local_store.create_new_collection(coll_id);

      std::string key_to_write = std::to_string(last_key_per_log[obj_num]);
      last_key_per_log[obj_num] += 1;

      std::string key_to_remove = std::to_string(first_key_per_log[obj_num]);
      first_key_per_log[obj_num] += 1;

      bufferlist val;
      val.append_zero(log_size);
      std::map<std::string, bufferlist> key_val;
      key_val[key_to_write] = val;

      ceph::os::Transaction one_write_delete;
      one_write_delete.omap_setkeys(coll_id, object, key_val);
      one_write_delete.omap_rmkey(coll_id, object, key_to_remove);

      auto latency_start = seastar::lowres_clock::now();
      co_await local_store.do_transaction(coll_ref,
                                          std::move(one_write_delete));
      auto latency_end = seastar::lowres_clock::now();

      auto time_millisec =
          std::chrono::duration_cast<std::chrono::milliseconds>(latency_end -
                                                                latency_start)
              .count();

      tot_latency += time_millisec;
      num_ops++;
    }
    co_return results_t{num_ops, tot_latency};
  };
  /**
   * This method returns a future
   * In this function we run the add_remove_entry function num_con_io times
   * simultaneously.
   *
   */

  auto run_concurrent_ios = [&]() -> seastar::future<> {
    std::vector<int> container_io;
    std::vector<results_t> all_io_res;
    for (int i = 0; i < num_concurrent_io; ++i) {
      container_io.push_back(i);
    }
    co_await seastar::parallel_for_each(
        container_io, ([&](int) -> seastar::future<> {
          auto res = co_await add_remove_entry();
          all_io_res.push_back(std::move(res));
          co_return;
        }));

    int tot_ops_all_io = 0;
    int tot_latency_all_io = 0;
    for (auto it = all_io_res.begin(); it != all_io_res.end(); ++it) {
      tot_ops_all_io += it->num_operations;
      tot_latency_all_io += it->tot_latency_per_io;
    }
    ERROR("Total number of operations performed across ios is {}",
          tot_ops_all_io);
    ERROR("Total latency aka time per operation, across ios is {}",
          tot_latency_all_io);
    ERROR("throughput, number of ops per millisec is {}",
          static_cast<double>(tot_ops_all_io) / duration);
    ERROR("average latency across ios is {}",
          tot_latency_all_io / tot_ops_all_io);
    co_return;
  };

  co_await pre_fill_logs();
  co_await run_concurrent_ios();
  co_return;
}

std::string generate_random_string(int key_size){
  std::string res="";
  for (int i = 0;i<key_size;++i){
    char letter=char(std::rand()%26 +97);
    res+=letter;
  }
  return res;
}

seastar::future<> rgw_index_workload(crimson::os::FuturizedStore &global_store,
                                  int num_indices, int num_concurrent_io,
                                  int duration, int key_size,
                                  int value_size,int target_keys_per_bucket,
                                  int tolerance_range) {

  LOG_PREFIX(rgw_index_workload);
  auto &local_store = global_store.get_sharded_store();
  std::map<int, coll_t> collection_id_for_rgw;
  std::vector<std::set<std::string>> keys_per_bucket;

  auto pre_fill_buckets=[&]()->seastar::future<>{
    for (int i=0;i<num_indices;++i){
      auto bucket_i = create_hobj(i);
      auto coll_id = make_cid(i);
      collection_id_for_rgw[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);

      std::map<std::string, bufferlist> omap_for_this_bucket;
      std::set<std::string> keys_in_this_bucket;

      for (int j = 0; j < target_keys_per_bucket; ++j) {
        std::string possible_key = generate_random_string(key_size);
        while (keys_in_this_bucket.count(possible_key)>0){
          possible_key=generate_random_string(key_size);
        }
        keys_in_this_bucket.insert(possible_key);
        bufferlist val_for_poss_key;
        val_for_poss_key.append_zero(value_size);
        omap_for_this_bucket[possible_key] = val_for_poss_key;
      }
      keys_per_bucket[i]=keys_in_this_bucket;
      ceph::os::Transaction txn_write_omap_for_bucket;
      txn_write_omap_for_bucket.create(coll_id, bucket_i);
      txn_write_omap_for_bucket.omap_setkeys(coll_id, bucket_i, omap_for_this_bucket);
      co_await local_store.do_transaction(coll_ref, std::move(txn_write_omap_for_bucket));
    }
    co_return;
  };

  std::vector<int>size_per_bucket(num_indices,target_keys_per_bucket);

  int min_size=std::floor(target_keys_per_bucket*(1-tolerance_range/100));
  int max_size=std::ceil(target_keys_per_bucket*(1+tolerance_range/100));

  auto rgw_actual_test=[&]()->seastar::future<results_t>{
    int num_ops = 0;
    int tot_latency = 0;
    auto start = seastar::lowres_clock::now();

    while (seastar::lowres_clock::now() - start <=
           std::chrono::milliseconds(duration)) {

      int bucket_num_we_choose = std::rand() % num_indices;
      auto bucket = create_hobj(bucket_num_we_choose);
      auto coll_id = collection_id_for_rgw[bucket_num_we_choose];
      auto coll_ref = co_await local_store.create_new_collection(coll_id);

      int size_bucket_we_choose=size_per_bucket[bucket_num_we_choose];
      auto keys_in_that_bucket=keys_per_bucket[bucket_num_we_choose];

      while (min_size < size_bucket_we_choose && size_bucket_we_choose < max_size){
        int choice=std::rand()%2;
        //choice 0 is write, choice 1 is delete 
        if (choice==0){
          std::string new_key=generate_random_string(key_size);
          while (keys_in_that_bucket.count(new_key)>0){
            new_key=generate_random_string(key_size);
          }
          keys_in_that_bucket.insert(new_key);
          size_per_bucket[bucket_num_we_choose]++;

          bufferlist value;
          value.append_zero(value_size);

          std::map<std::string, bufferlist> data_entry;
          data_entry[new_key] = value;

          ceph::os::Transaction one_write;
          one_write.omap_setkeys(coll_id, bucket, data_entry);
          auto latency_start = seastar::lowres_clock::now();
          co_await local_store.do_transaction(coll_ref,
                                          std::move(one_write));
          auto latency_end = seastar::lowres_clock::now();
          auto time_per_write =
          std::chrono::duration_cast<std::chrono::milliseconds>(latency_end -
                                                                latency_start)
              .count();
          tot_latency += time_per_write;
          num_ops++;
        }
        else{
          int index_key_to_delete=std::rand()%keys_in_that_bucket.size();
          auto it=keys_in_that_bucket.begin();
          std::advance(it,index_key_to_delete);
          keys_in_that_bucket.erase(it);
          size_per_bucket[bucket_num_we_choose]--; 


          ceph::os::Transaction one_delete;
          one_delete.omap_rmkey(coll_id, bucket, *it);

          auto latency_start = seastar::lowres_clock::now();
          co_await local_store.do_transaction(coll_ref,
                                            std::move(one_delete));
          auto latency_end = seastar::lowres_clock::now();
          auto time_per_delete =
            std::chrono::duration_cast<std::chrono::milliseconds>(latency_end -
                                                                  latency_start)
                .count();
            tot_latency += time_per_delete;
            num_ops++;
        }


      };
      if (size_bucket_we_choose<min_size){
        while (size_bucket_we_choose<=min_size){
          std::string new_key=generate_random_string(key_size);
          while (keys_in_that_bucket.count(new_key)>0){
            new_key=generate_random_string(key_size);
          }
          keys_in_that_bucket.insert(new_key);
          size_per_bucket[bucket_num_we_choose]++;

          bufferlist value;
          value.append_zero(value_size);

          std::map<std::string, bufferlist> data_entry;
          data_entry[new_key] = value;

          ceph::os::Transaction one_write;
          one_write.omap_setkeys(coll_id, bucket, data_entry);
          auto latency_start = seastar::lowres_clock::now();
          co_await local_store.do_transaction(coll_ref,
                                          std::move(one_write));
          auto latency_end = seastar::lowres_clock::now();
          auto time_per_write =
          std::chrono::duration_cast<std::chrono::milliseconds>(latency_end -
                                                                latency_start)
              .count();
          tot_latency += time_per_write;
          num_ops++;

        }
      }
      else{
        int index_key_to_delete=std::rand()%keys_in_that_bucket.size();
        auto it=keys_in_that_bucket.begin();
        std::advance(it,index_key_to_delete);
        keys_in_that_bucket.erase(it);
        size_per_bucket[bucket_num_we_choose]--; 


        ceph::os::Transaction one_delete;
        one_delete.omap_rmkey(coll_id, bucket, *it);

        auto latency_start = seastar::lowres_clock::now();
        co_await local_store.do_transaction(coll_ref,
                                            std::move(one_delete));
        auto latency_end = seastar::lowres_clock::now();
        auto time_per_delete =
        std::chrono::duration_cast<std::chrono::milliseconds>(latency_end -
                                                                  latency_start)
                .count();
        tot_latency += time_per_delete;
        num_ops++;

      }

      }
    co_return results_t{num_ops,tot_latency};
    
    
  };



  };
                                



int main(int argc, char **argv) {
  LOG_PREFIX(main);
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;
  std::string io_pattern;
  int num_logs = 0;
  int log_length = 0;
  int log_size = 0;
  int num_concurrent_io = 0;
  int duration = 0;

  desc.add_options()("help,h", "show help message")(
      "store-type",
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
       "be a symlink to the target device for bluestore or seastore")(
          "debug", po::value<bool>(&debug)->default_value(false),
          "enable debugging");

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
  } catch (const po::error &e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-store-bench";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));

  std::vector<char *> av{argv[0]};
  std::transform(std::begin(unrecognized_options),
                 std::end(unrecognized_options), std::back_inserter(av),
                 [](auto &s) { return const_cast<char *>(s.c_str()); });
  app.add_options()("num_logs", po::value<int>(&num_logs),
                    "how many different logs we create; "
                    "aka, we create a log for every object, "
                    "so how many objects we create")

      ("log_length", po::value<int>(&log_length), "number of entries per log")

      ("num_concurrent_io", po::value<int>(&num_concurrent_io),
        "number of IOs happening simultaneously")

      ("duration", po::value<int>(&duration),
        "how long in milliseconds the actual testing loop runs for");

  return app.run(
      av.size(), av.data(),
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
       * I suggest having a look at
       * src/seastar/include/seastar/core/coroutine.hh for the implementation.
       * Note, the language guarrantees that *arguments* (whether to
       * a lambda or not) have their lifetimes extended for the duration
       * of the coroutine, so this isn't a problem for non-lambda
       * coroutines.
       */
      seastar::coroutine::lambda([&]() -> seastar::future<int> {
        if (debug) {
          seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::debug);
        } else {
          seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::error);
        }

        co_await crimson::common::sharded_conf().start(
            EntityName{}, std::string_view{"ceph"});
        co_await crimson::common::local_conf().start();

        {
          std::vector<const char *> cav;
          std::transform(
              std::begin(unrecognized_options), std::end(unrecognized_options),
              std::back_inserter(cav), [](auto &s) { return s.c_str(); });
          co_await crimson::common::local_conf().parse_argv(cav);
        }

        auto store = crimson::os::FuturizedStore::create(
            store_type, store_path,
            crimson::common::local_conf().get_config_values());

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
                std::format("error creating empty object store type {} in {}",
                            store_type, store_path)
                    .c_str()));
        co_await store->stop();

        co_await store->start();
        co_await store->mount().handle_error(
            crimson::stateful_ec::assert_failure(
                std::format("error mounting object store type {} in {}",
                            store_type, store_path)
                    .c_str()));
        std::vector<seastar::future<>> per_shard_futures;
        for (unsigned i = 0; i < seastar::smp::count; ++i) {
          per_shard_futures.push_back(seastar::smp::submit_to(
              i, seastar::coroutine::lambda(
                     [&, &store_ref = *store]() -> seastar::future<> {
                       ERROR("running example_io on reactor {}",
                             seastar::this_shard_id());
                       co_await pg_log_workload(store_ref, num_logs, log_length,
                                                log_size, num_concurrent_io,
                                                duration);
                     }))

          );
        }
        co_await seastar::when_all(per_shard_futures.begin(),
                                   per_shard_futures.end());
        co_await store->umount();
        co_await store->stop();
        co_await crimson::common::sharded_conf().stop();
        co_return 0;
      }));
}
