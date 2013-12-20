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

#ifndef ECBMSGTYPES_H
#define ECBMSGTYPES_H

#include "osd_types.h"
#include "include/buffer.h"
#include "os/ObjectStore.h"

struct ECSubWrite {
  tid_t tid;
  osd_reqid_t reqid;
  hobject_t soid;
  pg_stat_t stats;
  ObjectStore::Transaction t;
  eversion_t at_version;
  eversion_t trim_to;
  vector<pg_log_entry_t> log_entries;
  set<hobject_t> temp_removed;
  set<hobject_t> temp_added;
  ECSubWrite() {}
  ECSubWrite(
    tid_t tid,
    osd_reqid_t reqid,
    hobject_t soid,
    pg_stat_t stats,
    ObjectStore::Transaction t,
    eversion_t at_version,
    eversion_t trim_to,
    vector<pg_log_entry_t> log_entries,
    const set<hobject_t> &temp_removed,
    const set<hobject_t> &temp_added)
    : tid(tid), reqid(reqid), soid(soid), stats(stats), t(t),
      at_version(at_version),
      trim_to(trim_to), log_entries(log_entries),
      temp_removed(temp_removed),
      temp_added(temp_added) {}
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(ECSubWrite)

struct ECSubWriteReply {
  tid_t tid;
  bool committed;
  bool applied;
  ECSubWriteReply() : committed(false), applied(false) {}
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(ECSubWriteReply)

struct ECSubRead {
  tid_t tid;
  list<pair<hobject_t, pair<uint64_t, uint64_t> > > to_read;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(ECSubRead)

struct ECSubReadReply {
  tid_t tid;
  list<pair<hobject_t, pair<uint64_t, bufferlist> > > buffers_read;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(ECSubReadReply)

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWrite &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubWriteReply &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubRead &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubReadReply &rhs);

#endif
