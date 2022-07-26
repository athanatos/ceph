// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include "boost/intrusive_ptr.hpp"

template<typename T> using Ref = boost::intrusive_ptr<T>;

using core_id_t = unsigned;
static constexpr core_id_t NULL_CORE = std::numeric_limits<core_id_t>::max();
