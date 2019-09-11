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


#ifndef CEPH_MOSDOP_H
#define CEPH_MOSDOP_H

#include <atomic>

#include "MOSDFastDispatchOp.h"
#include "include/ceph_features.h"
#include "common/hobject.h"
#include "common/mClockCommon.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class OSD;

struct pre_dispatch_t {
  __u32 osdmap_epoch = 0;
  __u32 flags = 0;
  __u32 hobj_hash = 0;
  spg_t pgid;
  osd_reqid_t reqid; // reqid explicitly set by sender
  std::optional<ceph::qos::mclock_profile_params_t> mclock_profile_params;
  std::optional<ceph::qos::dmclock_request_t> dmclock_request_state;

  void encode(ceph::buffer::list &p) const {
    ENCODE_START(1, 1, p);
    encode(osdmap_epoch, p);
    encode(flags, p);
    encode(hobj_hash, p);
    encode(pgid, p);
    encode(reqid, p);
    encode(mclock_profile_params, p);
    encode(dmclock_request_state, p);
    ENCODE_FINISH(p);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(osdmap_epoch, p);
    decode(flags, p);
    decode(hobj_hash, p);
    decode(pgid, p);
    decode(reqid, p);
    decode(mclock_profile_params, p);
    decode(dmclock_request_state, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(pre_dispatch_t)

class MOSDOp : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 9;
  static constexpr int COMPAT_VERSION = 3;

  pre_dispatch_t pre_dispatch;

  uint32_t client_inc = 0;
  utime_t mtime;
  int32_t retry_attempt = -1;   // 0 is first attempt.  -1 if we don't know.

  hobject_t hobj;

  ceph::buffer::list::const_iterator p;
  // Decoding flags. Decoding is only needed for messages caught by pipe reader.
  // Transition from true -> false without locks being held
  // Can never see final_decode_needed == false and partial_decode_needed == true
  std::atomic<bool> partial_decode_needed;
  std::atomic<bool> final_decode_needed;
  //
public:
  std::vector<OSDOp> ops;
private:
  snapid_t snap_seq;
  std::vector<snapid_t> snaps;

  uint64_t features;
  bool bdata_encode;

public:
  friend class MOSDOpReply;

  ceph_tid_t get_client_tid() { return header.tid; }
  void set_snapid(const snapid_t& s) {
    hobj.snap = s;
  }
  void set_snaps(const std::vector<snapid_t>& i) {
    snaps = i;
  }
  void set_snap_seq(const snapid_t& s) { snap_seq = s; }
  void set_reqid(const osd_reqid_t rid) {
    pre_dispatch.reqid = rid;
  }
  void set_spg(spg_t p) {
    pre_dispatch.pgid = p;
  }

  // Fields decoded in partial decoding
  pg_t get_pg() const {
    ceph_assert(!partial_decode_needed);
    return pre_dispatch.pgid.pgid;
  }
  spg_t get_spg() const override {
    ceph_assert(!partial_decode_needed);
    return pre_dispatch.pgid;
  }
  pg_t get_raw_pg() const {
    ceph_assert(!partial_decode_needed);
    return pg_t(pre_dispatch.hobj_hash, pre_dispatch.pgid.pgid.pool());
  }
  epoch_t get_map_epoch() const override {
    ceph_assert(!partial_decode_needed);
    return pre_dispatch.osdmap_epoch;
  }
  int get_flags() const {
    ceph_assert(!partial_decode_needed);
    return pre_dispatch.flags;
  }
  osd_reqid_t get_reqid() const {
    ceph_assert(!partial_decode_needed);
    if (pre_dispatch.reqid.name != entity_name_t() || pre_dispatch.reqid.tid != 0) {
      return pre_dispatch.reqid;
    } else {
      if (!final_decode_needed)
	ceph_assert(pre_dispatch.reqid.inc == (int32_t)client_inc);  // decode() should have done this
      return osd_reqid_t(get_orig_source(),
                         pre_dispatch.reqid.inc,
			 header.tid);
    }
  }

  // Fields decoded in final decoding
  int get_client_inc() const {
    ceph_assert(!final_decode_needed);
    return client_inc;
  }
  utime_t get_mtime() const {
    ceph_assert(!final_decode_needed);
    return mtime;
  }
  object_locator_t get_object_locator() const {
    ceph_assert(!final_decode_needed);
    if (hobj.oid.name.empty())
      return object_locator_t(hobj.pool, hobj.nspace, pre_dispatch.hobj_hash);
    else
      return object_locator_t(hobj);
  }
  const object_t& get_oid() const {
    ceph_assert(!final_decode_needed);
    return hobj.oid;
  }
  const hobject_t &get_hobj() const {
    return hobj;
  }
  snapid_t get_snapid() const {
    ceph_assert(!final_decode_needed);
    return hobj.snap;
  }
  const snapid_t& get_snap_seq() const {
    ceph_assert(!final_decode_needed);
    return snap_seq;
  }
  const std::vector<snapid_t> &get_snaps() const {
    ceph_assert(!final_decode_needed);
    return snaps;
  }

  /**
   * get retry attempt
   *
   * 0 is the first attempt.
   *
   * @return retry attempt, or -1 if we don't know
   */
  int get_retry_attempt() const {
    return retry_attempt;
  }
  uint64_t get_features() const {
    if (features)
      return features;
    return get_connection()->get_features();
  }

  const auto &get_mclock_profile_params() const {
    return pre_dispatch.mclock_profile_params;
  }

  void set_mclock_profile_params(
    ceph::qos::mclock_profile_params_t mclock_profile_params) {
    pre_dispatch.mclock_profile_params = mclock_profile_params;
  }

  const auto &get_dmclock_request_state() const {
    return pre_dispatch.dmclock_request_state;
  }

  void set_dmclock_request_state(
    ceph::qos::dmclock_request_t dmclock_request_state) {
    pre_dispatch.dmclock_request_state = dmclock_request_state;
  }

  MOSDOp()
    : MOSDFastDispatchOp(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      partial_decode_needed(true),
      final_decode_needed(true),
      bdata_encode(false) { }
  MOSDOp(int inc, long tid, const hobject_t& ho, spg_t& _pgid,
	 epoch_t _osdmap_epoch,
	 int _flags, uint64_t feat,
	 std::optional<ceph::qos::mclock_profile_params_t> profile_params = std::nullopt,
	 std::optional<ceph::qos::dmclock_request_t> dmclock_request_state = std::nullopt)
    : MOSDFastDispatchOp(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      pre_dispatch{_osdmap_epoch, ho.get_hash(), (__u32)_flags, _pgid, osd_reqid_t(),
		   profile_params, dmclock_request_state},
      client_inc(inc),
      hobj(ho),
      partial_decode_needed(false),
      final_decode_needed(false),
      features(feat),
      bdata_encode(false) {
    set_tid(tid);

    // also put the client_inc in reqid.inc, so that get_reqid() can
    // be used before the full message is decoded.
    pre_dispatch.reqid.inc = inc;
  }
private:
  ~MOSDOp() override {}

public:
  void set_mtime(utime_t mt) { mtime = mt; }
  void set_mtime(ceph::real_time mt) {
    mtime = ceph::real_clock::to_timespec(mt);
  }

  // ops
  void add_simple_op(int o, uint64_t off, uint64_t len) {
    OSDOp osd_op;
    osd_op.op.op = o;
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    ops.push_back(osd_op);
  }
  void write(uint64_t off, uint64_t len, ceph::buffer::list& bl) {
    add_simple_op(CEPH_OSD_OP_WRITE, off, len);
    data.claim(bl);
    header.data_off = off;
  }
  void writefull(ceph::buffer::list& bl) {
    add_simple_op(CEPH_OSD_OP_WRITEFULL, 0, bl.length());
    data.claim(bl);
    header.data_off = 0;
  }
  void zero(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_ZERO, off, len);
  }
  void truncate(uint64_t off) {
    add_simple_op(CEPH_OSD_OP_TRUNCATE, off, 0);
  }
  void remove() {
    add_simple_op(CEPH_OSD_OP_DELETE, 0, 0);
  }

  void read(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_READ, off, len);
  }
  void stat() {
    add_simple_op(CEPH_OSD_OP_STAT, 0, 0);
  }

  bool has_flag(__u32 flag) const { return pre_dispatch.flags & flag; };

  bool is_retry_attempt() const { return pre_dispatch.flags & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(unsigned a) { 
    if (a)
      pre_dispatch.flags |= CEPH_OSD_FLAG_RETRY;
    else
      pre_dispatch.flags &= ~CEPH_OSD_FLAG_RETRY;
    retry_attempt = a;
  }

  // marshalling
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if( false == bdata_encode ) {
      OSDOp::merge_osd_op_vector_in_data(ops, data);
      bdata_encode = true;
    }

    if ((features & CEPH_FEATURE_OBJECTLOCATOR) == 0) {
      // here is the old structure we are encoding to: //
#if 0
struct ceph_osd_request_head {
	__le32 client_inc;                 /* client incarnation */
	struct ceph_object_layout layout;  /* pgid */
	__le32 osdmap_epoch;               /* client's osdmap epoch */

	__le32 flags;

	struct ceph_timespec mtime;        /* for mutations only */
	struct ceph_eversion reassert_version; /* if we are replaying op */

	__le32 object_len;     /* length of object name */

	__le64 snapid;         /* snapid to read */
	__le64 snap_seq;       /* writer's snap context */
	__le32 num_snaps;

	__le16 num_ops;
	struct ceph_osd_op ops[];  /* followed by ops[], obj, ticket, snaps */
} __attribute__ ((packed));
#endif
      header.version = 1;

      encode(client_inc, payload);

      __u32 su = 0;
      encode(get_raw_pg(), payload);
      encode(su, payload);

      encode(pre_dispatch.osdmap_epoch, payload);
      encode(pre_dispatch.flags, payload);
      encode(mtime, payload);
      encode(eversion_t(), payload);  // reassert_version

      __u32 oid_len = hobj.oid.name.length();
      encode(oid_len, payload);
      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      __u32 num_snaps = snaps.size();
      encode(num_snaps, payload);
      
      //::encode(ops, payload);
      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      ceph::encode_nohead(hobj.oid.name, payload);
      ceph::encode_nohead(snaps, payload);
    } else if ((features & CEPH_FEATURE_NEW_OSDOP_ENCODING) == 0) {
      header.version = 6;
      encode(client_inc, payload);
      encode(pre_dispatch.osdmap_epoch, payload);
      encode(pre_dispatch.flags, payload);
      encode(mtime, payload);
      encode(eversion_t(), payload); // reassert_version
      encode(get_object_locator(), payload);
      encode(get_raw_pg(), payload);

      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
        encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
      if (pre_dispatch.reqid.name != entity_name_t() || pre_dispatch.reqid.tid != 0) {
	encode(pre_dispatch.reqid, payload);
      } else {
	// don't include client_inc in the reqid for the legacy v6
	// encoding or else we'll confuse older peers.
	encode(osd_reqid_t(), payload);
      }
    } else if (!HAVE_FEATURE(features, RESEND_ON_SPLIT)) {
      // reordered, v7 message encoding
      header.version = 7;
      encode(get_raw_pg(), payload);
      encode(pre_dispatch.osdmap_epoch, payload);
      encode(pre_dispatch.flags, payload);
      encode(eversion_t(), payload); // reassert_version
      encode(pre_dispatch.reqid, payload);
      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
    } else if (!HAVE_FEATURE(features, INDEPENDENT_PRE_DISPATCH)) {
      // v8 encoding with hobject_t hash separate from pre_dispatch.pgid, no
      // reassert version
      header.version = 8;

      encode(pre_dispatch.pgid, payload);
      encode(hobj.get_hash(), payload);
      encode(pre_dispatch.osdmap_epoch, payload);
      encode(pre_dispatch.flags, payload);
      encode(pre_dispatch.reqid, payload);
      encode_trace(payload, features);

      // -- above decoded up front; below decoded post-dispatch thread --

      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
    } else {
      // latest v9 encoding with independent pre-dispatch struct
      header.version = HEAD_VERSION;

      encode(pre_dispatch, payload);
      encode_trace(payload, features);

      // -- above decoded up front; below decoded post-dispatch thread --

      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
    }
  }

  void decode_payload() override {
    using ceph::decode;
    ceph_assert(partial_decode_needed && final_decode_needed);
    p = std::cbegin(payload);

    // Always keep here the newest version of decoding order/rule
    if (header.version == HEAD_VERSION) {
      decode(pre_dispatch, p);
      decode_trace(p);
    } else if (header.version == 8) {
      decode(pre_dispatch.pgid, p);      // actual pre_dispatch.pgid
      decode(pre_dispatch.hobj_hash, p);
      decode(pre_dispatch.osdmap_epoch, p);
      decode(pre_dispatch.flags, p);
      decode(pre_dispatch.reqid, p);
      decode_trace(p);
    } else if (header.version == 7) {
      decode(pre_dispatch.pgid.pgid, p);      // raw pgid
      pre_dispatch.hobj_hash = pre_dispatch.pgid.pgid.ps();
      decode(pre_dispatch.osdmap_epoch, p);
      decode(pre_dispatch.flags, p);
      eversion_t reassert_version;
      decode(reassert_version, p);
      decode(pre_dispatch.reqid, p);
    } else if (header.version < 2) {
      // old decode
      decode(client_inc, p);

      old_pg_t opgid;
      ceph::decode_raw(opgid, p);
      pre_dispatch.pgid.pgid = opgid;

      __u32 su;
      decode(su, p);

      decode(pre_dispatch.osdmap_epoch, p);
      decode(pre_dispatch.flags, p);
      decode(mtime, p);
      eversion_t reassert_version;
      decode(reassert_version, p);

      __u32 oid_len;
      decode(oid_len, p);
      decode(hobj.snap, p);
      decode(snap_seq, p);
      __u32 num_snaps;
      decode(num_snaps, p);
      
      //::decode(ops, p);
      __u16 num_ops;
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	decode(ops[i].op, p);

      ceph::decode_nohead(oid_len, hobj.oid.name, p);
      ceph::decode_nohead(num_snaps, snaps, p);

      // recalculate pre_dispatch.pgid hash value
      pre_dispatch.pgid.pgid.set_ps(
	ceph_str_hash(CEPH_STR_HASH_RJENKINS,
		      hobj.oid.name.c_str(),
		      hobj.oid.name.length()));
      hobj.pool = pre_dispatch.pgid.pgid.pool();
      pre_dispatch.hobj_hash = pre_dispatch.pgid.pgid.ps();
      hobj.set_hash(pre_dispatch.hobj_hash);

      retry_attempt = -1;
      features = 0;
      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in pre_dispatch.reqid.inc for get_reqid()'s benefit
      pre_dispatch.reqid = osd_reqid_t();
      pre_dispatch.reqid.inc = client_inc;
    } else if (header.version < 7) {
      decode(client_inc, p);
      decode(pre_dispatch.osdmap_epoch, p);
      decode(pre_dispatch.flags, p);
      decode(mtime, p);
      eversion_t reassert_version;
      decode(reassert_version, p);

      object_locator_t oloc;
      decode(oloc, p);

      if (header.version < 3) {
	old_pg_t opgid;
	ceph::decode_raw(opgid, p);
	pre_dispatch.pgid.pgid = opgid;
      } else {
	decode(pre_dispatch.pgid.pgid, p);
      }

      decode(hobj.oid, p);

      //::decode(ops, p);
      __u16 num_ops;
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
        decode(ops[i].op, p);

      decode(hobj.snap, p);
      decode(snap_seq, p);
      decode(snaps, p);

      if (header.version >= 4)
        decode(retry_attempt, p);
      else
        retry_attempt = -1;

      if (header.version >= 5)
        decode(features, p);
      else
	features = 0;

      if (header.version >= 6)
	decode(pre_dispatch.reqid, p);
      else
	pre_dispatch.reqid = osd_reqid_t();

      hobj.pool = pre_dispatch.pgid.pgid.pool();
      hobj.set_key(oloc.key);
      hobj.nspace = oloc.nspace;
      pre_dispatch.hobj_hash = pre_dispatch.pgid.pgid.ps();
      hobj.set_hash(pre_dispatch.hobj_hash);

      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in pre_dispatch.reqid.inc for get_reqid()'s benefit
      if (pre_dispatch.reqid.name == entity_name_t() && pre_dispatch.reqid.tid == 0)
	pre_dispatch.reqid.inc = client_inc;
    }

    partial_decode_needed = false;
  }

  bool finish_decode() {
    using ceph::decode;
    ceph_assert(!partial_decode_needed); // partial decoding required
    if (!final_decode_needed)
      return false; // Message is already final decoded
    ceph_assert(header.version >= 7);

    decode(client_inc, p);
    decode(mtime, p);
    object_locator_t oloc;
    decode(oloc, p);
    decode(hobj.oid, p);

    __u16 num_ops;
    decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      decode(ops[i].op, p);

    decode(hobj.snap, p);
    decode(snap_seq, p);
    decode(snaps, p);

    decode(retry_attempt, p);

    decode(features, p);

    hobj.pool = pre_dispatch.pgid.pgid.pool();
    hobj.set_key(oloc.key);
    hobj.nspace = oloc.nspace;

    OSDOp::split_osd_op_vector_in_data(ops, data);

    hobj.set_hash(pre_dispatch.hobj_hash);
    final_decode_needed = false;
    return true;
  }

  void clear_buffers() override {
    OSDOp::clear_data(ops);
    bdata_encode = false;
  }

  std::string_view get_type_name() const override { return "osd_op"; }
  void print(std::ostream& out) const override {
    out << "osd_op(";
    if (!partial_decode_needed) {
      out << get_reqid() << ' ';
      out << pre_dispatch.pgid;
      if (!final_decode_needed) {
	out << ' ';
	out << hobj
	    << " " << ops
	    << " snapc " << get_snap_seq() << "=" << snaps;
	if (is_retry_attempt())
	  out << " RETRY=" << get_retry_attempt();
      } else {
	out << " " << get_raw_pg() << " (undecoded)";
      }
      out << " " << ceph_osd_flag_string(get_flags());
      out << " e" << pre_dispatch.osdmap_epoch;
    }
    out << ")";
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};


#endif
