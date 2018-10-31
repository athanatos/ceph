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

#ifndef MSPPGACCEPT_H
#define MSPPGACCEPT_H

#include "MOSDFastDispatchOp.h"
#include "paxospg/protocol/multi/msg_types.h"

class MSPPGAccept : public MessageInstance<MSPPGAccept, MOSDFastDispatchOp> {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t epoch = 0;
  PaxosPG::Protocol::MultiPaxos::instance_num_t instance = 0;

  int get_cost() const override {
    return 0;
  }
  epoch_t get_map_epoch() const override {
    return epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MSPPGAccept()
    : MessageInstance(MSG_OSD_PPG_ACCEPT, HEAD_VERSION, COMPAT_VERSION)
    {}

  void decode_payload() override {
/*
    bufferlist::iterator p = payload.begin();
    ::decode(pgid, p);
    ::decode(epoch, p);
    ::decode(instance, p);
    */
  }

  void encode_payload(uint64_t features) override {
/*
    ::encode(pgid, payload);
    ::encode(epoch, payload);
    ::encode(instance, payload);
    encode_trace(payload, features);
    */
  }

  const char *get_type_name() const override { return "MSPPGAccept"; }

  void print(ostream& out) const override {
    out << "MSPPGAccept(" << pgid
	<< " " << epoch
	<< " " << instance
        << ")";
  }
};

#endif
