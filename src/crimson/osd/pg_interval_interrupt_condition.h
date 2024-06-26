// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <iostream>

#include "include/types.h"
#include "osd/osd_types.h"

#include "crimson/common/errorator.h"
#include "crimson/common/exception.h"
#include "crimson/common/type_helpers.h"

namespace crimson::osd {

class PG;

class IOInterruptCondition {
public:
  IOInterruptCondition(Ref<PG>& pg);
  
  struct disable_interval_t {};
  IOInterruptCondition(disable_interval_t, Ref<PG>& pg);

  ~IOInterruptCondition();

  bool new_interval_created();

  bool is_stopping();

  bool is_primary();

  template <typename Fut>
  std::optional<Fut> may_interrupt() {
    if (new_interval_created()) {
      return seastar::futurize<Fut>::make_exception_future(
        ::crimson::common::actingset_changed(is_primary()));
    }
    if (is_stopping()) {
      return seastar::futurize<Fut>::make_exception_future(
        ::crimson::common::system_shutdown_exception());
    }
    return std::optional<Fut>();
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, ::crimson::common::actingset_changed>
    || std::is_same_v<T, ::crimson::common::system_shutdown_exception>;

  static bool is_interruption(std::exception_ptr& eptr) {
    return (*eptr.__cxa_exception_type() ==
            typeid(::crimson::common::actingset_changed) ||
            *eptr.__cxa_exception_type() ==
            typeid(::crimson::common::system_shutdown_exception));
  }

  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext & ctx) const {
    return fmt::format_to(
      ctx.out(), "IOInterruptCondition({}, pgid:{}, e:{})", (void*)this,
      pgid, e);
  }

private:
  Ref<PG> pg;
  const spg_t pgid;
  std::optional<epoch_t> e;
};

} // namespace crimson::osd
