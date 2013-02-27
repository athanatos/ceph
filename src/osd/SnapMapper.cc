// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "SnapMapper.h"

using std::string;

const string SnapMapper::MAPPING_PREFIX = "MAP_";
const string SnapMapper::OBJECT_PREFIX = "OBJ_";

struct Mapping {
  uint64_t snap;
  hobject_t hoid;
  Mapping(const pair<uint64_t, hobject_t> &in)
    : snap(in.first), hoid(in.second) {}
  Mapping() : snap(0) {}
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(snap, bl);
    ::encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(snap, bl);
    ::decode(hoid, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(Mapping);

string SnapMapper::get_prefix(snapid_t snap)
{
  char buf[100];
  int len = snprintf(
    buf, sizeof(buf),
    "%.*X_", (int)(sizeof(snap)*2),
    static_cast<unsigned>(snap));
  return MAPPING_PREFIX + string(buf, len);
}

string SnapMapper::to_raw_key(
  const pair<snapid_t, hobject_t> &in)
{
  return get_prefix(in.first) + in.second.to_str();
}

pair<string, bufferlist> SnapMapper::to_raw(
  const pair<snapid_t, hobject_t> &in)
{
  bufferlist bl;
  ::encode(Mapping(in), bl);
  return make_pair(
    to_raw_key(in),
    bl);
}

pair<SnapMapper::snapid_t, hobject_t> SnapMapper::from_raw(
  const pair<std::string, bufferlist> &image)
{
  Mapping map;
  bufferlist bl(image.second);
  bufferlist::iterator bp(bl.begin());
  ::decode(map, bp);
  return make_pair(map.snap, map.hoid);
}

bool SnapMapper::is_mapping(const string &to_test)
{
  return to_test.substr(0, MAPPING_PREFIX.size()) == MAPPING_PREFIX;
}

string SnapMapper::to_object_key(const hobject_t &hoid)
{
  return OBJECT_PREFIX + hoid.to_str();
}

void SnapMapper::object_snaps::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(oid, bl);
  ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapMapper::object_snaps::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(oid, bl);
  ::decode(snaps, bl);
  DECODE_FINISH(bl);
}

int SnapMapper::get_snaps(
  const hobject_t &oid,
  object_snaps *out)
{
  set<string> keys;
  map<string, bufferlist> got;
  keys.insert(to_object_key(oid));
  int r = backend.get_keys(keys, &got);
  if (r < 0)
    return r;
  if (got.size() == 0)
    return -ENOENT;
  if (out) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(*out, bp);
  }
  return 0;
}

void SnapMapper::set_snaps(
  const hobject_t &oid,
  const object_snaps &in,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  map<string, bufferlist> to_set;
  bufferlist bl;
  ::encode(in, bl);
  to_set[to_object_key(oid)] = bl;
  backend.set_keys(to_set, t);
}

void SnapMapper::update_snaps(
  const hobject_t &oid,
  set<snapid_t> new_snaps,
  set<snapid_t> old_snaps,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    assert(r == 0);
    assert(out.snaps == old_snaps);
  }
  object_snaps snaps(oid, new_snaps);
  set_snaps(oid, snaps, t);

  set<string> to_remove;
  for (set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!new_snaps.count(*i)) {
      to_remove.insert(to_raw_key(make_pair(*i, oid)));
    }
  }
  backend.remove_keys(to_remove, t);
}

void SnapMapper::add_oid(
  const hobject_t &oid,
  set<snapid_t> snaps,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    assert(r == -ENOENT);
  }

  object_snaps _snaps(oid, snaps);
  set_snaps(oid, _snaps, t);

  map<string, bufferlist> to_add;
  for (set<snapid_t>::iterator i = snaps.begin();
       i != snaps.end();
       ++i) {
    to_add.insert(to_raw(make_pair(*i, oid)));
  }
  backend.set_keys(to_add, t);
}

int SnapMapper::get_next_object_to_trim(
  snapid_t snap,
  hobject_t *hoid)
{
  string list_after(get_prefix(snap));
  pair<string, bufferlist> next;
  int r = backend.get_next(list_after, &next);
  if (r < 0) {
    return r; // -ENOENT indicates no next
  }

  if (!is_mapping(next.first)) {
    return -ENOENT;
  }

  pair<snapid_t, hobject_t> next_decoded(from_raw(next));
  if (next_decoded.first != snap) {
    return -ENOENT;
  }

  if (hoid)
    *hoid = next_decoded.second;
  return 0;
}
