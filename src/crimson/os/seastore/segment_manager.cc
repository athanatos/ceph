#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"

#ifdef HAVE_ZNS
#include "crimson/os/seastore/segment_manager/zns.h"
#endif

static seastar::future<crimson::os::seastore::SegmentManagerRef>
SegmentManager::get_segment_manager(
  const std::string &device)
{
#ifdef HAVE_ZNS
  return seastar::do_with(
    static_cast<size_t>(0),
    [&](auto &nr_zones) {
      return seastar::open_file_dma(
	device + "/block",
	seastar::open_flags::rw
      ).then([&](auto file) {
	return seastar::do_with(
	  file,
	  [=](auto &f) -> seastar::future<int> {
	    ceph_assert(f);
	    return f.ioctl(BLKGETNRZONES, (void *)&nr_zones);
	  });
      }).then([&](auto ret) -> crimson::os::seastore::SegmentManagerRef {
	crimson::os::seastore::SegmentManagerRef sm;
	if (nr_zones != 0) {
	  return std::make_unique<
	    segment_manager::zns::ZNSSegmentManager
	    >(device + "/block");
	} else {
	  return std::make_unique<
	    segment_manager::block::BlockSegmentManager
	    >(device + "/block");
	}
      });
    });
#else
  return seastar::make_ready_future<crimson::os::seastore::SegmentManagerRef>(
    std::make_unique<
      segment_manager::block::BlockSegmentManager
    >(device + "/block"));
#endif
}