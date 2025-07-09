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
 /* Writes to FuturizedStore instances are performed via
   * the ceph::os::Transaction class.  All operations within
   * the transaction will be applied atomically -- after a
   * failure either the whole transaction will have happened
   * or none of it will.
   *
   * ceph::os::Transaction is shared with classic because it
   * is part of the primary->replica replication protocol.
   * See os/Transaction.h for details.
   
  //lets start with assuming i have log size and number of logs frm CL
  std::vector<double>latencies;
  int num_operations=0;
  for (int i=0;i<num_logs;++i){
    auto obj_i = create_hobj(i);
    std::string key(std::to_string(i));
    bufferlist bl_new;
    bl_new.append_zero(log_entry_size);
  {
    ceph::os::Transaction t;
    t.create(cid, obj_i);
    std::map<std::string, bufferlist> attrs;
    attrs[key] = bl;
    t.omap_setkeys(
      cid,
      obj_i,
      attrs);
    // actually submit the transaction and await commit
    //we wrap this in a time block to see how long it takes this is our latency for this trasaction which just writes an object 
    auto start=seastar::lowres_clock::now();
    co_await local_store.do_transaction(
      coll_ref,
      std::move(t));
    ERROR("created object {} in collection {} with omap {}->{}",
          obj_i, cid, key, bl_new);
    auto end=seastar::lowres_clock::now();
    auto time_micro_sec=std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    latencies.push_back(time_micro_sec);
    num_operations++;
  }
}*/

#include <random>
#include <vector>

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

#include "crimson/common/coroutine.h"
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace ceph;

SET_SUBSYS(osd);


seastar::future<> pg_log_workload(crimson::os::FuturizedStore &global_store,int num_logs,int num_concurrent_io,int duration,int log_size,int log_length)
{
  LOG_PREFIX(pg_log_workload);
  auto &local_store = global_store.get_sharded_store();

  // since i need each object to have its own pg, each object has a different cid 
  auto make_cid=[](int obj_id){
    return coll_t (spg_t(pg_t(seastar::this_shard_id(), 0)));
  };

  auto create_hobj = [](unsigned id) {
    return ghobject_t(
      shard_id_t::NO_SHARD,
      seastar::this_shard_id(),
      id, // hash, normally rjenkins of name, but let's just set it to id
      "", // namespace, empty here
      "", // name, empty here
      0,  // snapshot
      ghobject_t::NO_GEN);
  };

  //creates a unique object 
  

  //a way for me to access both the num_operations and the latency_per_io
  //returned when i run the actual test 
  struct results{
    int num_operations;
    int tot_latency_per_io;
  };

  // stores the collection_id of each object so i can reference them later when i need to create a ref for an object that already exists 
  //here i'm assuming that creating a ref on the same coll id in multiple places gives the same result 

  std::map<int,coll_t>collection_id;
  
  //this method is suppossed to prefill N logs with M different entries each of size K 

  auto pre_fill_logs = [&]() -> seastar::future<>{
    for (int i = 0; i < num_logs; ++i) {
      auto obj_i = create_hobj(i);
      auto coll_id= make_cid(i);
      collection_id[i]=coll_id;
      auto coll_ref=co_await local_store.create_new_collection(coll_id);
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

  //Create two vectores to keep track of the last added key and the first added key
  std::vector<int>first_key_per_log(num_logs,0);
  std::vector<int> last_key_per_log(num_logs,log_length);
  
  //after the logs have been prefilled we do time constrained testing 
  // in duration amount of time how many adds and deletes can i do 
  // adds happen in the ned , deletes happend from the beginning
  
  auto actual_test=[&]()-> seastar::future<results>{
    int num_ops=0;
    int tot_latency=0;
    auto start=seastar::lowres_clock::now();

    while(seastar::lowres_clock::now()-start<= std::chrono::milliseconds(duration)){
      int obj_num=std::rand()%num_logs; 
      auto object=create_hobj(obj_num);
      auto coll_id=collection_id[obj_num];
      auto coll_ref=co_await local_store.create_new_collection(coll_id);

      std::string key_to_write=std::to_string(last_key_per_log[obj_num]); 
      last_key_per_log[obj_num]+=1;

      std::string key_to_remove=std::to_string(first_key_per_log[obj_num]);
      first_key_per_log[obj_num]+=1;

      bufferlist val;
      val.append_zero(log_size);
      std::map<std::string, bufferlist> key_val;
      key_val[key_to_write]=val;

      ceph::os::Transaction one_write_delete;
      one_write_delete.omap_setkeys(coll_id,object,key_val);
      one_write_delete.omap_rmkey(coll_id,object,key_to_remove);
  
      auto latency_start=seastar::lowres_clock::now();
      co_await local_store.do_transaction(coll_ref,std::move(one_write_delete));
      auto latency_end=seastar::lowres_clock::now();
      auto time_millisec=std::chrono::duration_cast<std::chrono::milliseconds>(latency_end-latency_start).count();
      tot_latency+=time_millisec;
      num_ops++;

    }
    co_return results{num_ops,tot_latency};
  };
  auto run_concurrent_ios=[&]() -> seastar::future<> {
    std::vector <int> container_io; // to make the num of concurrent operations a container like a vector otherwise cant use parallel loop 
    std::vector <results> all_io_res;
    for (int i=0;i<num_concurrent_io;++i){
      container_io.push_back(i);
    }
  co_await seastar::parallel_for_each(container_io,([&](int) -> seastar::future<> {
    auto res = co_await actual_test();   
    all_io_res.push_back(std::move(res));
    co_return;
  })
);

    int tot_ops_all_io=0;
    int tot_latency_all_io=0;
    for(auto it=all_io_res.begin();it!=all_io_res.end();++it){
      tot_ops_all_io+=it->num_operations;
      tot_latency_all_io+=it->tot_latency_per_io;
    }
    ERROR ("Total number of operations performed across ios is {}", tot_ops_all_io);
    ERROR("Total latency aka time per operation, across ios is {}", tot_latency_all_io);
    ERROR("throughput is {}", static_cast<double>(tot_ops_all_io)/duration);
    ERROR("average latency across ios is {}", tot_latency_all_io/tot_ops_all_io);
    co_return;
  
  };
  co_await pre_fill_logs();
  co_await run_concurrent_ios();
  co_return;
}

  

int main(int argc, char** argv)
{
  LOG_PREFIX(main);
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;
  std::string io_pattern;
  int num_logs=0;
  int log_length=0;
  int log_size=0;
  int num_concurrent_io=0;
  int duration=0; //duration in millisec

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
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-store-bench";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));

  std::vector<char*> av{argv[0]};
  std::transform(
    std::begin(unrecognized_options),
    std::end(unrecognized_options),
    std::back_inserter(av),
    [](auto& s) {
      return const_cast<char*>(s.c_str());
    });
  app.add_options()
  ("num_logs",po::value<int>(&num_logs),"how many different logs's we create, aka we create a log for every object so how many objects we create ")
  ("log_size", po::value<int>(&log_size),"number of bytes in the bufferlist stored in omap")
  ("log_length", po::value<int>(&log_length),"number of entries per log")
  ("num_concurrent_io", po::value<int>(&num_concurrent_io),"number of io's happening simulataneously")
  ("duration", po::value<int>(&duration),"how long in milliseconds does the actual testing loop run for");

  

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
        seastar::log_level::error
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
    co_await store->stop();

    co_await store->start();
    co_await store->mount().handle_error(
      crimson::stateful_ec::assert_failure(
        std::format(
          "error mounting object store type {} in {}",
          store_type,
          store_path).c_str()));
    std::vector<seastar::future<>>per_shard_futures;
    for (unsigned i = 0; i < seastar::smp::count; ++i) {
      per_shard_futures.push_back(
        seastar::smp::submit_to(
        i,
        seastar::coroutine::lambda(
          [&, &store_ref=*store]() -> seastar::future<> {  
            ERROR("running example_io on reactor {}", seastar::this_shard_id());
            co_await pg_log_workload(store_ref,num_logs,log_length,log_size,num_concurrent_io,duration);
          })
      )

      );
     
    }
    co_await seastar::when_all(per_shard_futures.begin(),per_shard_futures.end());

    co_await store->umount();
    co_await store->stop();
    co_await crimson::common::sharded_conf().stop();
    co_return 0;
  }));
}
