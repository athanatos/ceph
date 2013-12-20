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

#include "ECUtil.h"
#include "ECBackend.h"
#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <iostream>
#include <sstream>

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}


PGBackend::RecoveryHandle *open_recovery_op()
{
  return 0;
}

void ECBackend::run_recovery_op(
  RecoveryHandle *h,
  int priority)
{
}

void ECBackend::recover_object(
  const hobject_t &hoid,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *h)
{
}

bool ECBackend::handle_message(
  OpRequestRef _op)
{
  dout(10) << __func__ << ": " << _op << dendl;
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(_op->get_req());
    pg_shard_t from(op->get_source().num(), op->pgid.shard);
    handle_sub_write(from, op->op);
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    MOSDECSubOpWriteReply *op = static_cast<MOSDECSubOpWriteReply*>(
      _op->get_req());
    pg_shard_t from(op->get_source().num(), op->pgid.shard);
    handle_sub_write_reply(from, op->op);
    return true;
  }
  case MSG_OSD_EC_READ: {
    MOSDECSubOpRead *op = static_cast<MOSDECSubOpRead*>(_op->get_req());
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    pg_shard_t from(op->get_source().num(), op->pgid.shard);
    reply->pgid = from;
    reply->map_epoch = get_parent()->get_epoch();
    handle_sub_read(from, op->op, &(reply->op));
    get_parent()->send_message_osd_cluster(
      from.osd, reply, get_parent()->get_epoch());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_req());
    pg_shard_t from(op->get_source().num(), op->pgid.shard);
    handle_sub_read_reply(from, op->op);
    return true;
  }
  default:
    return false;
  }
  return false;
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  ECSubWrite &op)
{
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  ECSubRead &op,
  ECSubReadReply *reply)
{
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  ECSubWriteReply &op)
{
  map<tid_t, Op>::iterator i = tid_to_op_map.find(op.tid);
  assert(i != tid_to_op_map.end());
  if (op.committed) {
    i->second.pending_commit.erase(from);
  }
  if (op.applied) {
    i->second.pending_apply.erase(from);
  }
  check_pending_ops();
}

void ECBackend::handle_sub_read_reply(
  pg_shard_t from,
  ECSubReadReply &op)
{
  map<tid_t, ReadOp>::iterator iter = tid_to_read_map.find(op.tid);
  assert(iter != tid_to_read_map.end());
  assert(iter->second.in_progress.count(from));
  iter->second.complete[from].swap(op.buffers_read);
  iter->second.in_progress.erase(from);
  if (!iter->second.in_progress.empty())
    return;
  // done
  ReadOp &readop = iter->second;
  map<pg_shard_t,
      list<pair<hobject_t, pair<uint64_t, bufferlist> > >::iterator
      > res_iters;
  list<
    pair<
      hobject_t,
      boost::tuple<uint64_t, uint64_t, bufferlist*>
      >
    >::iterator out_iter;

  for (map<pg_shard_t,
	   list<pair<hobject_t, pair<uint64_t, bufferlist> > >
	 >::iterator i = readop.complete.begin();
       i != readop.complete.end();
       ++i) {
    assert(i->second.size() == readop.to_read.size());
    res_iters.insert(make_pair(i->first, i->second.begin()));
  }
  out_iter = readop.to_read.begin();

  while (true) {
    if (res_iters.begin()->second == readop.complete.begin()->second.end())
      break;
    uint64_t off(res_iters.begin()->second->second.first);
    hobject_t hoid(res_iters.begin()->second->first);
    map<int, bufferlist> chunks;
    for (map<pg_shard_t,
	   list<pair<hobject_t, pair<uint64_t, bufferlist> > >::iterator
	   >::iterator i = res_iters.begin();
	 i != res_iters.end();
	 ++i) {
      assert(i->second->first == hoid);
      assert(i->second->second.first == off);
      chunks[i->first.shard].claim(i->second->second.second);
      ++(i->second);
    }
    bufferlist decoded;
    int r = ECUtil::decode(
      stripe_size, stripe_width, ec_impl, chunks,
      &decoded);
    assert(r == 0);
    out_iter->second.get<2>()->substr_of(
      decoded,
      out_iter->second.get<0>() - ECUtil::logical_to_prev_stripe_bound_obj(
	stripe_size, stripe_width, out_iter->second.get<0>()),
      out_iter->second.get<1>());
  }
  readop.on_complete->complete(0);
  tid_to_read_map.erase(iter);
}

void ECBackend::check_recovery_sources(const OSDMapRef osdmap)
{
}

void ECBackend::_on_change(ObjectStore::Transaction *t)
{
}

void ECBackend::clear_state()
{
  waiting.clear();
  reading.clear();
  writing.clear();
  tid_to_op_map.clear();
  tid_to_read_map.clear();
}

void ECBackend::on_flushed()
{
}


void ECBackend::dump_recovery_info(Formatter *f) const
{
}

PGBackend::PGTransaction *ECBackend::get_transaction()
{
  return new ECTransaction;
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const eversion_t &at_version,
  PGTransaction *_t,
  const eversion_t &trim_to,
  vector<pg_log_entry_t> &log_entries,
  Context *on_local_applied_sync,
  Context *on_all_applied,
  Context *on_all_commit,
  tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
  )
{
  assert(!tid_to_op_map.count(tid));
  Op *op = &(tid_to_op_map[tid]);
  op->hoid = hoid;
  op->version = at_version;
  op->trim_to = trim_to;
  op->log_entries.swap(log_entries);
  op->on_local_applied_sync = on_local_applied_sync;
  op->on_all_applied = on_all_applied;
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;

  op->t = static_cast<ECTransaction*>(_t);
  op->t->populate_deps(
    stripe_width,
    &(op->must_read),
    &(op->writes));
  waiting.push_back(op);
  check_pending_ops();
}

void ECBackend::start_read_op(
  tid_t tid,
  const list<
    pair<
      hobject_t,
      boost::tuple<uint64_t, uint64_t, bufferlist*>
      >
    > &to_read,
  Context *onfinish)
{
  assert(!tid_to_read_map.count(tid));
  ReadOp &op(tid_to_read_map[tid]);
  op.to_read = to_read;
  op.in_progress = min_to_read;
  op.on_complete = onfinish;

  ECSubRead readmsg;
  readmsg.tid = tid;
  for (list<
	 pair<
	   hobject_t,
	   boost::tuple<uint64_t, uint64_t, bufferlist*>
	   >
	 >::const_iterator i = to_read.begin();
       i != to_read.end();
       ++i) {
    uint64_t obj_offset =
      ECUtil::logical_to_prev_stripe_bound_obj(
	stripe_size, stripe_width,
	i->second.get<0>());
    uint64_t obj_end =
      ECUtil::logical_to_next_stripe_bound_obj(
	stripe_size, stripe_width,
	i->second.get<0>());
    uint64_t obj_len = obj_end - obj_offset;
    readmsg.to_read.push_back(
      make_pair(
	i->first,
	make_pair(obj_offset, obj_len)));
  }
  for (set<pg_shard_t>::iterator i = op.in_progress.begin();
       i != op.in_progress.end();
       ++i) {
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->pgid = get_parent()->whoami_shard();
    msg->map_epoch = get_parent()->get_epoch();
    msg->op = readmsg;
    get_parent()->send_message_osd_cluster(
      i->osd,
      msg,
      get_parent()->get_epoch());
  }
}

void ECBackend::call_commit_apply_cbs()
{
  bool found_not_applied = false;
  bool found_not_committed = false;
  for (list<Op*>::iterator i = writing.begin();
       i != writing.end() && !(found_not_applied && found_not_committed);
       ++i) {
    if (!found_not_committed && (*i)->pending_commit.empty()) {
      if ((*i)->on_all_commit) {
	(*i)->on_all_commit->complete(0);
	(*i)->on_all_commit = 0;
      }
    } else {
      found_not_committed = true;
    }
    if (!found_not_applied && (*i)->pending_apply.empty()) {
      if ((*i)->on_all_applied) {
	(*i)->on_all_applied->complete(0);
	(*i)->on_all_applied = 0;
      }
    } else {
      found_not_applied = true;
    }
  }
}

bool ECBackend::can_read(Op *op) {
  for (set<hobject_t>::iterator i = op->writes.begin();
       i != op->writes.end();
       ++i) {
    if (unstable.count(*i))
      return false;
  }
  return true;
}

struct ReadCB : public Context {
  ECBackend *pg;
  ECBackend::Op *op;

  ReadCB(ECBackend *pg, ECBackend::Op *op) : pg(pg), op(op) {}
  void finish(int r) {
    assert(r == 0);
    op->must_read.clear();
    pg->check_pending_ops();
  }
};

void ECBackend::start_read(Op *op) {
  unstable.insert(op->writes.begin(), op->writes.end());
  if (op->must_read.empty())
    return;
  list<
    pair<pair<uint64_t, uint64_t>,
	 pair<bufferlist*, Context*> > > to_read;
  for (map<hobject_t, uint64_t>::iterator i = op->must_read.begin();
       i != op->must_read.end();
       ++i) {
    map<hobject_t, pair<uint64_t, bufferlist> >::iterator iter =
      op->reads_completed.insert(
	make_pair(
	  op->hoid,
	  make_pair(
	    i->second,
	    bufferlist()))).first;
    to_read.push_back(
      make_pair(
	make_pair(i->second, stripe_width),
	make_pair(&(iter->second.second), (Context*)0)));
  }

  objects_read_async(
    op->hoid,
    to_read,
    new ReadCB(this, op));
}

void ECBackend::start_write(Op *op) {
  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (get_parent()->should_send_op(i->shard, op->hoid))
      trans[i->shard];
  }
  op->t->generate_transactions(
    ec_impl,
    coll,
    temp_coll,
    stripe_width,
    stripe_size,
    op->reads_completed,
    &trans,
    &(op->temp_added),
    &(op->temp_cleared));

  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (!get_parent()->should_send_op(i->shard, op->hoid))
      continue;
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    assert(iter != trans.end());
    ECSubWrite sop(
      op->tid,
      op->reqid,
      iter->second,
      op->version,
      op->trim_to,
      op->log_entries);
    if (get_parent()->pgb_is_primary())
      do_write(sop, op->on_local_applied_sync);
    else
      send_write(*i, sop);
    op->on_local_applied_sync = 0;
  }
}

void ECBackend::do_write(
  ECSubWrite &write,
  Context *on_local_applied_sync)
{
}

void ECBackend::send_write(
  pg_shard_t to,
  ECSubWrite &write)
{
}

void ECBackend::check_pending_ops()
{
  call_commit_apply_cbs();
  while (!writing.empty()) {
    Op *op = writing.front();
    if (op->pending_commit.size() || op->pending_apply.size())
      break;
    for (set<hobject_t>::iterator i = op->writes.begin();
	 i != op->writes.end();
	 ++i) {
      assert(unstable.count(*i));
      unstable.erase(*i);
    }
    op->writes.clear();
    writing.pop_front();
  }

  while (!waiting.empty()) {
    Op *op = waiting.front();
    if (can_read(op)) {
      start_read(op);
      waiting.pop_front();
      reading.push_back(op);
    } else {
      break;
    }
  }

  while (!reading.empty()) {
    Op *op = reading.front();
    if (op->must_read.empty()) {
      start_write(op);
      reading.pop_front();
      writing.push_back(op);
    } else {
      break;
    }
  }
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  bufferlist *bl)
{
  return -EOPNOTSUPP;
}

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<pair<uint64_t, uint64_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  return;
}
