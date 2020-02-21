// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

namespace crimson::os::seastore {

Cache::replay_delta_ret
Cache::replay_delta(const delta_info_t &delta)
{
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}


}
