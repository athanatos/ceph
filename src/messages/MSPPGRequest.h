// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MSPPGREQUEST_H
#define MSPPGREQUEST_H

#include "MOSDFastDispatchOp.h"
#include "paxospg/protocol/multi/msg_types.h"

class MSPPGRequest : public MOSDFastDispatchOp {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  ceph_tid_t tid = 0;
  bufferlist ranked_encoding;
  spg_t pgid;
  epoch_t epoch = 0;

  int get_cost() const override {
    return 0;
  }
  epoch_t get_map_epoch() const override {
    return epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MSPPGRequest(ceph_tid_t tid, bufferlist bl)
    : MOSDFastDispatchOp(MSG_OSD_PPG_REQUEST, HEAD_VERSION, COMPAT_VERSION),
      tid(tid), ranked_encoding(bl)
    {}

  MSPPGRequest()
    : MSPPGRequest(0, bufferlist())
  {}

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(tid, p);
    ::decode(ranked_encoding, p);
    ::decode(pgid, p);
    ::decode(epoch, p);
  }

  void encode_payload(uint64_t features) override {
    ::encode(tid, payload);
    ::encode(ranked_encoding, payload);
    ::encode(pgid, payload);
    ::encode(epoch, payload);
    encode_trace(payload, features);
  }

  const char *get_type_name() const override { return "MSPPGRequest"; }

  void print(ostream& out) const override {
    out << "MSPPGRequest(" << pgid
	<< " " << epoch
        << ")";
  }
};

#endif
