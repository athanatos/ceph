#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <linux/fs.h>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

#include <string>

#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/segment_manager/zns.h"

#include "include/uuid.h"

#include "test/crimson/seastar_runner.h"

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
        app_cfg.name = "crimson-simple-store";
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


                        crimson::os::seastore::seastore_meta_t meta = {
                                uuid_d()
                        };

                        auto segment_manager = crimson::os::seastore::segment_manager::zns::ZNSSegmentManager("/dev/nvme0n2");
                        segment_manager.mkfs(meta);
                        should_stop.wait().get();
                        return 0;
                });
        });
}