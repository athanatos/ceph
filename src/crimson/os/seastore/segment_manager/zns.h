// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "crimson/common/layout.h"

#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore::segment_manager::zns {

	struct zns_sm_metadata_t {
		size_t size = 0;
		size_t segment_size = 0;
		size_t segment_capacity = 0;
		size_t zones_per_segment = 0;
		size_t zone_capacity = 0;
		size_t block_size = 0;
		size_t segments = 0;
		uint64_t first_segment_offset = 0;
		seastore_meta_t meta;
		
		DENC(zns_sm_metadata_t, v, p) {
			DENC_START(1, 1, p);
			denc(v.size, p);
			denc(v.segment_size, p);
			denc(v.zone_capacity, p);
			denc(v.zones_per_segment, p);
			denc(v.block_size, p);
			denc(v.segments, p);
			denc(v.first_segment_offset, p);
			denc(v.meta, p);
			DENC_FINISH(p);
		}
	};

	using write_ertr = crimson::errorator<crimson::ct_error::input_output_error>;
	using read_ertr = crimson::errorator<crimson::ct_error::input_output_error>;

	class ZNSSegmentManager;

	class ZNSSegment final : public Segment {
	public:
		ZNSSegment(ZNSSegmentManager &manager, segment_id_t id);

		segment_id_t get_segment_id() const final { return id; }
		segment_off_t get_write_capacity() const final;
		segment_off_t get_write_ptr() const final { return write_pointer; }
		close_ertr::future<> close() final;
		write_ertr::future<> write(segment_off_t offset, ceph::bufferlist bl) final;

		~ZNSSegment() {}
	private:
		friend class ZNSSegmentManager;
		ZNSSegmentManager &manager;
		const segment_id_t id;
		segment_off_t write_pointer = 0;
	
	};

	class ZNSSegmentManager final : public SegmentManager{
	public:
	  ZNSSegmentManager(const std::string&) {}
		mount_ret mount() final;

		mkfs_ret mkfs(seastore_meta_t meta) final;

		open_ertr::future<SegmentRef> open(segment_id_t id) final;

		release_ertr::future<> release(segment_id_t id) final;

		read_ertr::future<> read(paddr_t addr, size_t len, ceph::bufferptr &out) final;


	  size_t get_size() const final {
	    // TODO
	    return 0;
	  }

	  segment_off_t get_block_size() const final {
	    // TODO
	    return 0;
	  }
	    

	  segment_off_t get_segment_size() const final {
	    // TODO
	    return 0;
	  }

	  seastore_meta_t meta;
	  const seastore_meta_t &get_meta() const final {
	    return meta;
	  }

	private:
		friend class ZNSSegment;
		std::string device_path;
		zns_sm_metadata_t metadata;
  		seastar::file device;

		struct effort_t {
			uint64_t num = 0;
			uint64_t bytes = 0;

			void increment(uint64_t read_bytes) {
				++num;
				bytes += read_bytes;
			}
		};

		struct {
			effort_t data_read;
			effort_t data_write;
			effort_t metadata_write;
			uint64_t opened_segments;
			uint64_t closed_segments;
			uint64_t closed_segments_unused_bytes;
			uint64_t released_segments;

			void reset() {
				data_read = {};
				data_write = {};
				metadata_write = {};
				opened_segments = 0;
				closed_segments = 0;
				closed_segments_unused_bytes = 0;
				released_segments = 0;
			}
		} stats;

		void register_metrics();
		seastar::metrics::metric_group metrics;
	};
}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::segment_manager::zns::zns_sm_metadata_t
)
