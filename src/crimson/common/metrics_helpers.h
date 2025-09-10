// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>

#include <seastar/core/scollectd_api.hh>

#include "common/ceph_time.h"
#include "common/Formatter.h"

using namespace std::literals;

namespace crimson {
namespace metrics {

using registered_metric = seastar::metrics::impl::registered_metric;
using data_type = seastar::metrics::impl::data_type;
using value_map = seastar::metrics::impl::value_map;

class op_stats_t {
  struct counters_t {
    uint64_t op_count = 0;
    uint64_t op_size_total = 0;
    std::chrono::duration<double> op_latency_total = 0.0s;

    uint64_t op_queue_depth_total = 0;

    std::chrono::duration<double> time_idle = 0.0s;
    std::chrono::duration<double> time_busy = 0.0s;

    void operator+=(const counters_t &rhs) {
      op_count += rhs.op_count;
      op_size_total += rhs.op_size_total;
      op_latency_total += rhs.op_latency_total;
      op_queue_depth_total += rhs.op_queue_depth_total;
      time_idle += rhs.time_idle;
      time_busy += rhs.time_busy;
    }

    void operator-=(const counters_t &rhs) {
      op_count -= rhs.op_count;
      op_size_total -= rhs.op_size_total;
      op_latency_total -= rhs.op_latency_total;
      op_queue_depth_total -= rhs.op_queue_depth_total;
      time_idle -= rhs.time_idle;
      time_busy -= rhs.time_busy;
    }
  };

  uint64_t current_queue_depth = 0;
  struct entry_t {
    ceph::mono_clock::time_point start;
    counters_t counters;
    entry_t(ceph::mono_clock::time_point start) : start(start) {}
  };

  ceph::mono_clock::time_point last_idle_accounting = ceph::mono_clock::now();
  std::deque<entry_t> counters;
  static constexpr std::chrono::duration<double> period = 1s;
  static constexpr std::chrono::duration<double> total = 10s;

  struct summary_t : counters_t {
    ceph::mono_clock::time_point start;
  };
  summary_t summary;

  std::string group_name;
  std::vector<seastar::metrics::label_instance> labels;
  seastar::metrics::metric_group metrics;

  void account_idle_time(ceph::mono_clock::time_point now) {
    auto to_account = now - last_idle_accounting;
    last_idle_accounting = now;
    if (current_queue_depth > 0) {
      counters.front().counters.time_busy += to_account;
      summary.time_busy += to_account;
    } else {
      counters.front().counters.time_idle += to_account;
      summary.time_idle += to_account;
    }
  }

  counters_t &get_counters() {
    assert(counters.size() > 0);
    auto now = ceph::mono_clock::now();
    if (now - counters.front().start > period) {
      account_idle_time(now);
      counters.emplace_front(now);
    }
    while (counters.size() > 1 && now - counters.back().start > total) {
      summary -= counters.back().counters;
      counters.pop_back();
      summary.start = counters.back().start;
    }
    assert(counters.size() > 0);
    return counters.front().counters;
  }

  struct accounter_t {
    ceph::mono_clock::time_point start = ceph::mono_clock::now();
    op_stats_t &parent;
    uint64_t size;
    uint64_t queue_depth_at_submission;
    accounter_t(op_stats_t &parent, uint64_t size)
      : parent(parent), size(size) {
      if (parent.current_queue_depth == 0) {
	parent.account_idle_time(ceph::mono_clock::now());
      }
      queue_depth_at_submission = ++parent.current_queue_depth;
    }
    ~accounter_t() {
      auto &counters = parent.get_counters();
      counters.op_count++;
      parent.summary.op_count++;
      counters.op_queue_depth_total += queue_depth_at_submission;
      parent.summary.op_queue_depth_total += queue_depth_at_submission;
      counters.op_latency_total += ceph::mono_clock::now() - start;
      parent.summary.op_latency_total += ceph::mono_clock::now() - start;
      counters.op_size_total += size;
      parent.summary.op_size_total += size;

      if (parent.current_queue_depth == 1) {
	parent.account_idle_time(ceph::mono_clock::now());
      }
      --parent.current_queue_depth;
    }
  };

public:
  op_stats_t(
    std::string group_name,
    std::vector<seastar::metrics::label_instance> labels)
    : group_name(group_name), labels(labels) {
    counters.emplace_back(last_idle_accounting);
    summary.start = last_idle_accounting;
  }

  void register_metrics() {
    namespace sm = seastar::metrics;
    metrics.add_group(
      group_name,
      {
	sm::make_gauge(
	  "op_count",
	  [this] {
	    return summary.op_count;
	  },
	  sm::description("op count"),
	  labels
	),
	sm::make_gauge(
	  "op_size_total",
	  [this] {
	    return summary.op_size_total;
	  },
	  sm::description("op size total"),
	  labels
	),
	sm::make_gauge(
	  "op_size_avarage",
	  [this] {
	    return static_cast<double>(summary.op_size_total) /
	      summary.op_count;
	  },
	  sm::description("average op size"),
	  labels
	),
	sm::make_gauge(
	  "op_latency_total_s",
	  [this] {
	    return summary.op_latency_total.count();
	  },
	  sm::description("total op latency"),
	  labels
	),
	sm::make_gauge(
	  "op_latency_average_s",
	  [this] {
	    return summary.op_latency_total.count() / summary.op_count;
	  },
	  sm::description("average op latency"),
	  labels
	),
	sm::make_gauge(
	  "op_queue_depth_total",
	  [this] {
	    return summary.op_queue_depth_total;
	  },
	  sm::description("sum of op queue depth at write submission time"),
	  labels
	),
	sm::make_gauge(
	  "op_queue_depth_average",
	  [this] {
	    return static_cast<double>(summary.op_queue_depth_total) /
	      summary.op_count;
	  },
	  sm::description("average write depth at write submission time"),
	  labels
	),
	sm::make_gauge(
	  "measurement_duration",
	  [this] {
	    return std::chrono::duration<double>(
	      ceph::mono_clock::now() - summary.start).count();
	  },
	  sm::description("time span for counters"),
	  labels
	),
	sm::make_gauge(
	  "idle_time",
	  [this] {
	    return summary.time_idle.count();
	  },
	  sm::description("idle time"),
	  labels
	),
	sm::make_gauge(
	  "idle_ratio",
	  [this] {
	    return summary.time_idle.count() /
	      std::chrono::duration<double>(
		ceph::mono_clock::now() - summary.start
	      ).count();
	  },
	  sm::description("time share spent idle"),
	  labels
	),
	sm::make_gauge(
	  "busy_time",
	  [this] {
	    return summary.time_busy.count();
	  },
	  sm::description("idle time"),
	  labels
	),
	sm::make_gauge(
	  "busy_ratio",
	  [this] {
	    return summary.time_busy.count() /
	      std::chrono::duration<double>(
		ceph::mono_clock::now() - summary.start
	      ).count();
	  },
	  sm::description("time share spent busy"),
	  labels
	),
      }
    );
  }

  accounter_t account_op(uint64_t size) {
    return accounter_t(*this, size);
  }

  const summary_t &get_summary() {
    return summary;
  }
};

static void dump_metric_value(
  Formatter* f,
  std::string_view full_name,
  const registered_metric& metric,
  const seastar::metrics::impl::labels_type& labels)
{
  f->open_object_section(full_name);
  for (const auto& [key, value] : labels) {
    f->dump_string(key, value);
  }
  auto value_name = "value";
  switch (auto v = metric(); v.type()) {
  case data_type::GAUGE:
    f->dump_float(value_name, v.d());
    break;
  case data_type::REAL_COUNTER:
    f->dump_float(value_name, v.d());
    break;
  case data_type::COUNTER:
    double val;
    try {
      val = v.ui();
    } catch (std::range_error&) {
      // seastar's cpu steal time may be negative
      val = 0;
    }
    f->dump_unsigned(value_name, val);
    break;
  case data_type::HISTOGRAM: {
    f->open_object_section(value_name);
    auto&& h = v.get_histogram();
    f->dump_float("sum", h.sample_sum);
    f->dump_unsigned("count", h.sample_count);
    f->open_array_section("buckets");
    for (auto i : h.buckets) {
      f->open_object_section("bucket");
      f->dump_float("le", i.upper_bound);
      f->dump_unsigned("count", i.count);
      f->close_section(); // "bucket"
    }
    {
      f->open_object_section("bucket");
      f->dump_string("le", "+Inf");
      f->dump_unsigned("count", h.sample_count);
      f->close_section();
    }
    f->close_section(); // "buckets"
    f->close_section(); // value_name
  }
    break;
  default:
    std::abort();
    break;
  }
  f->close_section(); // full_name
}

template <typename F>
void dump_metric_value_map(
  const value_map &vmap,
  Formatter *f,
  F &&filter)
{
  assert(f);
  for (const auto& [full_name, metric_family]: seastar::scollectd::get_value_map()) {
    if (!std::invoke(filter, full_name)) {
      continue;
    }
    for (const auto& [labels, metric] : metric_family) {
      if (metric && metric->is_enabled()) {
	f->open_object_section(""); // enclosed by array
	dump_metric_value(
	  f, full_name, *metric, labels.labels());
	f->close_section();
      }
    }
  }
}

}
}
