// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"

#define DOUT_SUBSYS optracker
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "--OSD::tracker-- ";
}

void OpTracker::dump_ops_in_flight(ostream &ss)
{
  JSONFormatter jf(true);
  Mutex::Locker locker(ops_in_flight_lock);
  jf.open_object_section("ops_in_flight"); // overall dump
  jf.dump_int("num_ops", ops_in_flight.size());
  jf.open_array_section("ops"); // list of OpRequests
  utime_t now = ceph_clock_now(g_ceph_context);
  for (xlist<OpRequest*>::iterator p = ops_in_flight.begin(); !p.end(); ++p) {
    stringstream name;
    Message *m = (*p)->request;
    m->print(name);
    jf.open_object_section("op");
    jf.dump_string("description", name.str().c_str()); // this OpRequest
    jf.dump_stream("received_at") << (*p)->received_time;
    jf.dump_float("age", now - (*p)->received_time);
    jf.dump_string("flag_point", (*p)->state_string());
    if (m->get_orig_source().is_client()) {
      jf.open_object_section("client_info");
      stringstream client_name;
      client_name << m->get_orig_source();
      jf.dump_string("client", client_name.str());
      jf.dump_int("tid", m->get_tid());
      jf.close_section(); // client_info
    }
    jf.close_section(); // this OpRequest
  }
  jf.close_section(); // list of OpRequests
  jf.close_section(); // overall dump
  jf.flush(ss);
}

void OpTracker::register_inflight_op(xlist<OpRequest*>::item *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  ops_in_flight.push_back(i);
  ops_in_flight.back()->seq = seq++;
}

void OpTracker::unregister_inflight_op(xlist<OpRequest*>::item *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  assert(i->get_list() == &ops_in_flight);
  i->remove_myself();
}

bool OpTracker::check_ops_in_flight(ostream &out)
{
  Mutex::Locker locker(ops_in_flight_lock);
  if (!ops_in_flight.size())
    return false;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t too_old = now;
  too_old -= g_conf->osd_op_complaint_time;
  
  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
	   << "; oldest is " << now - ops_in_flight.front()->received_time
	   << " seconds old" << dendl;
  xlist<OpRequest*>::iterator i = ops_in_flight.begin();
  while (!i.end() && (*i)->received_time < too_old) {
    // exponential backoff of warning intervals
    if ( ( (*i)->received_time +
	   (g_conf->osd_op_complaint_time *
	    (*i)->warn_interval_multiplier) )< now) {
      out << "old request " << *((*i)->request) << " received at "
	  << (*i)->received_time << " currently " << (*i)->state_string();
      (*i)->warn_interval_multiplier *= 2;
    }
    ++i;
  }
  return !i.end();
}

void OpTracker::mark_event(OpRequest *op, const string &dest)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  return _mark_event(op, dest, now);
}

void OpTracker::_mark_event(OpRequest *op, const string &evt,
			    utime_t time)
{
  Mutex::Locker locker(ops_in_flight_lock);
  dout(1) << "reqid: " << op->get_reqid() << ", seq: " << op->seq
	  << ", time: " << time << ", event: " << evt
	  << ", request: " << *op->request << dendl;
}

void OpTracker::RemoveOnDelete::operator()(OpRequest *op) {
  op->mark_event("done");
  tracker->unregister_inflight_op(&(op->xitem));
  delete op;
}

OpRequestRef OpTracker::create_request(Message *ref)
{
  OpRequestRef retval(new OpRequest(ref, this),
		      RemoveOnDelete(this));

  if (ref->get_type() == CEPH_MSG_OSD_OP) {
    retval->reqid = static_cast<MOSDOp*>(ref)->get_reqid();
  } else if (ref->get_type() == MSG_OSD_SUBOP) {
    retval->reqid = static_cast<MOSDSubOp*>(ref)->reqid;
  }
  _mark_event(retval.get(), "header_read", ref->get_recv_stamp());
  _mark_event(retval.get(), "throttled", ref->get_throttle_stamp());
  _mark_event(retval.get(), "all_read", ref->get_recv_complete_stamp());
  _mark_event(retval.get(), "dispatched", ref->get_dispatch_stamp());
  return retval;
}

void OpRequest::mark_event(const string &event)
{
  tracker->mark_event(this, event);
}
