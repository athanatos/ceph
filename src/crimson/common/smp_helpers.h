// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

#include <seastar/core/smp.hh>

#include "crimson/common/errorator.h"

namespace crimson {

using core_id_t = unsigned;
static constexpr core_id_t NULL_CORE = std::numeric_limits<core_id_t>::max();

template <typename Obj, typename Method, typename ArgTuple, size_t... I>
static auto apply_method_to_tuple(
  Obj &obj, Method method, ArgTuple &&tuple,
  std::index_sequence<I...>) {
  return (obj.*method)(std::get<I>(std::move(tuple))...);
}

static auto submit_to(core_id_t core, auto &&f) {
  using ret_type = decltype(f());
  if constexpr (is_errorated_future<ret_type>::value) {
    using ret_type = decltype(f());
    auto ret = seastar::smp::submit_to(
      core,
      [f=std::move(f)]() mutable {
	return f().to_base();
      });
    return ret_type(std::move(ret));
  } else {
    return seastar::smp::submit_to(core, std::move(f));
  }
}

}
