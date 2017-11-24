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

#ifndef MSPPGRESPONSE_H
#define MSPPGRESPONSE_H

#include "MOSDFastDispatchOp.h"
#include "paxospg/protocol/multi/msg_types.h"

class MSPPGResponse : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  ceph_tid_t tid = 0;
  bufferlist encoded_response;
  epoch_t epoch = 0;
  uint32_t hash = 0;

  int get_cost() const override {
    return 0;
  }
  epoch_t get_map_epoch() const {
    return epoch;
  }
  uint32_t get_hash() const {
    return hash;
  }

  MSPPGResponse()
    : Message(MSG_OSD_PPG_RESPONSE, HEAD_VERSION, COMPAT_VERSION)
    {}

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(tid, p);
    ::decode(encoded_response, p);
    ::decode(epoch, p);
    ::decode(hash, p);
  }

  void encode_payload(uint64_t features) override {
    ::encode(tid , payload);
    ::encode(encoded_response, payload);
    ::encode(epoch, payload);
    ::encode(hash, payload);
    encode_trace(payload, features);
  }

  const char *get_type_name() const override { return "MSPPGResponse"; }

  void print(ostream& out) const override {
    out << "MSPPGResponse(" << tid
	<< " " << encoded_response.length()
	<< " " << epoch
	<< " " << hash
        << ")";
  }
};

#endif
