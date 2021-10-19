#include <sys/mman.h>
#include <string.h>
#include <linux/blkzoned.h>

#include "crimson/os/seastore/segment_manager/zns.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "include/buffer.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::segment_manager::zns {
        // mount_ret ZNSSegmentManager::mount() {
        //         seastar::open_file_dma(
        //                 device_path,
        //                 seastar::open_flags::rw
        //         ).then((auto &device) {
        //                 auto bp = bufferptr(ceph::buffer::create_page_aligned(sd.block_size))
        //                 auto size = device.dma_read(
        //                         0,
        //                         bp, 
        //                         bp.len()
        //                 );
        //                 return size;
        //         }.then([=] (auto size) {
        //                 zns_sm_metadata_t metadata;
        //                 decode(metadata)
        //         })
        // }

        using check_create_device_ertr = ZNSSegmentManager::access_ertr;
        using check_create_device_ret = check_create_device_ertr::future<>;
        static check_create_device_ret check_create_device(
        const std::string &path,
        size_t size)
        {
                logger().error(
                "zns.cc:check_create_device path {}, size {}",
                path,
                size);
                return seastar::open_file_dma(
                        path,
                        seastar::open_flags::exclusive |
                        seastar::open_flags::rw |
                        seastar::open_flags::create
                ).then([size](auto file) {
                        return seastar::do_with(
                        file,
                        [size](auto &f) -> seastar::future<> {
                                logger().error(
                                "zns.cc:check_create_device: created device, truncating to {}",
                                size);
                                ceph_assert(f);
                                int nr_zones = 0;
                                return f.ioctl(BLKPBSZGET, &nr_zones)
                                .then([size, &nr_zones, &f] (int ret) {
                                        if (nr_zones == 0) return seastar::make_exception_future<>(std::system_error(std::make_error_code(std::errc::io_error)));
                                        return f.truncate(
                                                size
                                        ).then([&f, size] {
                                                return f.allocate(0, size);
                                        }).finally([&f] {
                                                return f.close();
                                        });
                                });
                        });
                }).then_wrapped([&path](auto f) -> check_create_device_ret {
                        logger().error(
                        "zns.cc:check_create_device: complete failed: {}",
                        f.failed());
                        if (f.failed()) {
                                try {
                                        f.get();
                                        return seastar::now();
                                } catch (const std::system_error &e) {
                                        if (e.code().value() == EEXIST) {
                                                logger().error(
                                                "zns.cc:check_create_device device {} exists",
                                                path);
                                                return seastar::now();
                                        } else {
                                        logger().error(
                                        "zns.cc:check_create_device device {} creation error {}",
                                        path,
                                        e);
                                        return crimson::ct_error::input_output_error::make();
                                        }
                                } catch (...) {
                                        return crimson::ct_error::input_output_error::make();
                                }
                        } else {
                                std::ignore = f.discard_result();
                                return seastar::now();
                        }
                });
        }

        using open_device_ret = 
                ZNSSegmentManager::access_ertr::future<
                std::pair<seastar::file, seastar::stat_data>
                >;
        static
        open_device_ret open_device(
                const std::string &path,
                seastar::open_flags mode)
        {
                return seastar::file_stat(path, seastar::follow_symlink::yes
                ).then([mode, &path](auto stat) mutable {
                        return seastar::open_file_dma(path, mode).then([=](auto file) {
                                logger().error(
                                        "open_device: open successful, size {}",
                                        stat.size
                                );
                                return std::make_pair(file, stat);
                        });
                }).handle_exception([](auto e) -> open_device_ret {
                        logger().error(
                        "open_device: got error {}",
                        e);
                        return crimson::ct_error::input_output_error::make();
                });
        }

        static
        zns_sm_metadata_t make_metadata(
        seastore_meta_t meta,
        const seastar::stat_data &data,
        size_t zone_size,
        size_t zone_capacity,
        size_t num_zones)
        {
                using crimson::common::get_conf;

                auto config_size = get_conf<Option::size_t>(
                "seastore_device_size");

                size_t size = (data.size == 0) ? config_size : data.size;

                auto config_segment_size = get_conf<Option::size_t>(
                "seastore_segment_size");

                size_t zones_per_segment = config_segment_size / zone_capacity;

                size_t segments = (num_zones - 1) * zones_per_segment;

                logger().debug(
                        "{}: size {}, block_size {}, allocated_size {}, configured_size {}, "
                        "segment_size {}",
                        __func__,
                        data.size,
                        data.block_size,
                        data.allocated_size,
                        config_size,
                        config_segment_size
                );        
                return zns_sm_metadata_t{
                        size,
                        config_segment_size,
                        zone_capacity * zones_per_segment,
                        zones_per_segment,
                        zone_capacity,
                        data.block_size,
                        segments,
                        zone_size * zones_per_segment,
                        meta
                };
        }

        static seastar::future<> get_zone_capacity(seastar::file &device, int zone_capacity, int zone_size, int nr_zones){
                struct blk_zone_range first_zone_range;
                first_zone_range.sector = 0;
                first_zone_range.nr_sectors = zone_size;
                //TODO: use do_with instead
                struct blk_zone_report *hdr;
                return device.ioctl(BLKFINISHZONE, &first_zone_range)
                .then([&] (int ret) {
                        int hdr_len = sizeof(struct blk_zone_report) + nr_zones * sizeof(struct blk_zone);
                        hdr = (struct blk_zone_report*) malloc(hdr_len);
                        return device.ioctl(BLKREPORTZONE, hdr);
                }).then([&] (int ret) {
                        zone_capacity = hdr->zones[0].wp;
                        return seastar::now();
                });
        }

        static write_ertr::future<> do_write(
                seastar::file &device,
                uint64_t offset,
                bufferptr &bptr)
        {
                logger().debug(
                        "zns: do_write offset {} len {}",
                        offset,
                        bptr.length());
                return device.dma_write(
                        offset,
                        bptr.c_str(),
                        bptr.length()
                ).handle_exception([](auto e) -> write_ertr::future<size_t> {
                        logger().error(
                                "do_write: dma_write got error {}",
                                e);
                        return crimson::ct_error::input_output_error::make();
                }).then([length=bptr.length()](auto result)-> write_ertr::future<> {
                        if (result != length) {
                                return crimson::ct_error::input_output_error::make();
                        }
                        return write_ertr::now();
                });
        }
        
        static
        ZNSSegmentManager::access_ertr::future<>
        write_metadata(seastar::file &device, zns_sm_metadata_t sb)
        {
                assert(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>() <
                        sb.block_size);
                return seastar::do_with(
                        bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
                        [=, &device](auto &bp) {
                                bufferlist bl;
                                encode(sb, bl);
                                auto iter = bl.begin();
                                assert(bl.length() < sb.block_size);
                                iter.copy(bl.length(), bp.c_str());
                                logger().debug("write_metadata: doing writeout");
                                return do_write(device, 0, bp);
                        }
                );
        }       

ZNSSegmentManager::mount_ret ZNSSegmentManager::mount() {
  // TODO
  return mount_ertr::now();
}

ZNSSegmentManager::mkfs_ret ZNSSegmentManager::mkfs(seastore_meta_t meta) {
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    zns_sm_metadata_t{},
    [=](auto &device, auto &stat, auto &sb) {
      logger().error("ZNSSegmentManager::mkfs path {}", device_path);
      check_create_device_ret maybe_create = check_create_device_ertr::now();
      using crimson::common::get_conf;
      if (get_conf<bool>("seastore_zns_create")) {
	auto size = get_conf<Option::size_t>("seastore_device_size");
	maybe_create = check_create_device(device_path, size);
      }
      
      return maybe_create
	.safe_then([this] {
	  return open_device(device_path, seastar::open_flags::rw);
	}).safe_then([&, meta](auto p) {
	  auto device = p.first;
	  auto stat = p.second;
	  size_t zone_size, zone_capacity, nr_zones;
	  return device.ioctl(BLKGETZONESZ, (void *) &zone_size)
	    .then([&] (int ret) {
	      return device.ioctl(BLKGETNRZONES, (void *) &nr_zones);
	    })
	    .then([&] (int ret) {
	      return get_zone_capacity(device, zone_capacity, zone_size, nr_zones);
	    })
	    .then([&, meta, zone_size] {
	      sb = make_metadata(meta, stat, zone_size, zone_capacity, nr_zones);
	      stats.metadata_write.increment(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>());
	      return write_metadata(device, sb);
	    });
	}).finally([&] {
	  return device.close();
	}).safe_then([] {
	  logger().debug("ZNSSegmentManager::mkfs: complete");
	  return mkfs_ertr::now();
	});
    });
}

ZNSSegmentManager::open_ertr::future<SegmentRef>
ZNSSegmentManager::open(segment_id_t id) {
  // TODO
  return open_ertr::future<SegmentRef>(
    open_ertr::ready_future_marker{},
    SegmentRef());
}

ZNSSegmentManager::release_ertr::future<>
ZNSSegmentManager::release(segment_id_t id) {
  // TODO
  return release_ertr::now();
}

ZNSSegmentManager::read_ertr::future<>
ZNSSegmentManager::read(paddr_t addr, size_t len, ceph::bufferptr &out) {
  // TODO
  return release_ertr::now();
}

}
