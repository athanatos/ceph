// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECUTIL_H
#define ECUTIL_H

#include <map>
#include <set>

#include "ErasureCodeInterface.h"
#include "include/buffer.h"
#include "include/assert.h"

namespace ECUtil {

inline int decode(
  uint64_t stripe_size,
  uint64_t stripe_width,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out) {
  assert(to_decode.size());
  uint64_t obj_size = to_decode.begin()->second.length();
  uint64_t chunk_stripe_width = stripe_width / stripe_size;
  assert(obj_size % chunk_stripe_width == 0);
  assert(obj_size > 0);
  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == obj_size);
  }
  for (uint64_t i = 0; i < obj_size; i += chunk_stripe_width) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, chunk_stripe_width);
    }
    bufferlist bl;
    int r = ec_impl->decode_concat(chunks, &bl);
    assert(bl.length() == stripe_width);
    assert(r == 0);
    out->claim_append(bl);
  }
  return 0;
}

inline int decode(
  uint64_t stripe_size,
  uint64_t stripe_width,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out) {
  assert(to_decode.size());
  uint64_t obj_size = to_decode.begin()->second.length();
  uint64_t chunk_stripe_width = stripe_width / stripe_size;
  assert(obj_size % chunk_stripe_width == 0);
  assert(obj_size > 0);
  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == obj_size);
  }
  set<int> need;
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    need.insert(i->first);
  }
  for (uint64_t i = 0; i < obj_size; i += chunk_stripe_width) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, chunk_stripe_width);
      assert(chunks[j->first].length() == chunk_stripe_width);
    }
    map<int, bufferlist> out_bls;
    int r = ec_impl->decode(need, chunks, &out_bls);
    if (r < 0)
      return r;
    for (map<int, bufferlist*>::iterator j = out.begin();
	 j != out.end();
	 ++j) {
      assert(out_bls.count(j->first));
      assert(out_bls[j->first].length() == chunk_stripe_width);
      j->second->claim_append(out_bls[j->first]);
    }
  }
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second->length() == obj_size);
  }
  return 0;
}

inline int encode(
  uint64_t stripe_size,
  uint64_t stripe_width,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out) {
  uint64_t logical_size = in.length();
  assert(logical_size % stripe_width == 0);
  assert(logical_size > 0);
  for (uint64_t i = 0; i < logical_size; i += stripe_width) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, stripe_width);
    int r = ec_impl->encode(want, buf, &encoded);
    assert(r == 0);
    assert(encoded.begin()->second.length() == (stripe_width/stripe_size));
    for (map<int, bufferlist>::iterator i = encoded.begin();
	 i != encoded.end();
	 ++i) {
      (*out)[i->first].claim_append(i->second);
    }
  }
  assert(out->begin()->second.length() == logical_size / stripe_size);
  return 0;
}

inline uint64_t logical_to_prev_stripe_bound_obj(
  uint64_t stripe_size,
  uint64_t stripe_width,
  uint64_t logical_offset) {
  return (logical_offset / stripe_width) * (stripe_width / stripe_size);
}

inline uint64_t logical_to_next_stripe_bound_obj(
  uint64_t stripe_size,
  uint64_t stripe_width,
  uint64_t logical_offset) {
  return ((logical_offset + stripe_width - 1) / stripe_width) *
    (stripe_width / stripe_size);
}


inline uint64_t obj_bound_to_logical_offset(
  uint64_t stripe_size,
  uint64_t stripe_width,
  uint64_t obj_offset) {
  assert(obj_offset % stripe_size == 0);
  return (obj_offset / stripe_size) * stripe_width;
}

inline uint64_t logical_to_prev_stripe_bound(
  uint64_t stripe_width,
  uint64_t logical_offset) {
  return (logical_offset / stripe_width) * stripe_width;
}

inline uint64_t logical_to_next_stripe_bound(
  uint64_t stripe_width,
  uint64_t logical_offset) {
  return ((logical_offset + stripe_width - 1) / stripe_width) *
    (stripe_width);
}

inline pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
  uint64_t stripe_width,
  const pair<uint64_t, uint64_t> &in) {
  uint64_t off = logical_to_prev_stripe_bound(stripe_width, in.first);
  uint64_t len = logical_to_next_stripe_bound(
    stripe_width, in.second + (in.first - off));
  return make_pair(off, len);
}

inline pair<uint64_t, uint64_t> aligned_offset_len_to_chunk(
  uint64_t stripe_width,
  uint64_t chunk_size,
  const pair<uint64_t, uint64_t> &in) {
  assert(in.first % stripe_width == 0);
  assert(in.second % stripe_width == 0);
  return make_pair(
    (in.first / stripe_width) * chunk_size,
    (in.second / stripe_width) * chunk_size);
}
};
#endif
