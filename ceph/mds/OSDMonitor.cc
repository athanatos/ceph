// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#include "OSDMonitor.h"

#include "osd/OSDMap.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDBoot.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << "mon" << whoami << " e" << (osdmap ? osdmap->get_epoch():0) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << "mon" << whoami << " e" << (osdmap ? osdmap->get_epoch():0) << " "


class C_OM_PingTick : public Context {
public:
  Messenger *msgr;
  C_OM_PingTick(Messenger *m) : msgr(m) {}
  void finish(int r) {
	msgr->send_message(new MPing, MSG_ADDR_MON(0));
  }
};

class C_OM_Faker : public Context {
public:
  OSDMonitor *om;
  C_OM_Faker(OSDMonitor *m) { 
	this->om = m;
  }
  void finish(int r) {
	om->fake_reorg();
  }
};

class C_OM_FakeOSDFailure : public Context {
  OSDMonitor *mon;
  int osd;
  bool down;
public:
  C_OM_FakeOSDFailure(OSDMonitor *m, int o, bool d) : mon(m), osd(o), down(d) {}
  void finish(int r) {
	mon->fake_osd_failure(osd,down);
  }
};




void OSDMonitor::fake_reorg() 
{
  
  // HACK osd map change
  static int d = 0;

  if (d > 0) {
	dout(1) << "changing OSD map, marking osd" << d-1 << " out" << endl;
	osdmap->mark_out(d-1);
  }

  dout(1) << "changing OSD map, marking osd" << d << " down" << endl;
  osdmap->mark_down(d);

  osdmap->inc_epoch();
  d++;
  
  // bcast
  bcast_latest_osd_map_osd();
    
  // do it again?
  if (g_conf.num_osd - d > 4 &&
	  g_conf.num_osd - d > g_conf.num_osd/2)
	g_timer.add_event_after(g_conf.fake_osdmap_expand,
							new C_OM_Faker(this));
}


void OSDMonitor::init()
{
  dout(1) << "init" << endl;


  // <HACK set up OSDMap from g_conf>
  osdmap = new OSDMap();
  osdmap->set_pg_bits(g_conf.osd_pg_bits);
  osdmap->inc_epoch();  // = 1
  assert(osdmap->get_epoch() == 1);


  //if (g_conf.mkfs) osdmap->set_mkfs();

  Bucket *b = new UniformBucket(1, 0);
  int root = osdmap->crush.add_bucket(b);
  for (int i=0; i<g_conf.num_osd; i++) {
	osdmap->osds.insert(i);
	b->add_item(i, 1);
  }
  
  for (int i=1; i<5; i++) {
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 0));
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
  }

  if (g_conf.mds_local_osd) {
	// add mds osds, but don't put them in the crush mapping func
	for (int i=0; i<g_conf.num_mds; i++) 
	  osdmap->osds.insert(i+10000);
  }

  osdmap->encode(maps[osdmap->get_epoch()]); // 1
  pending.epoch = osdmap->get_epoch()+1;     // 2
  // </HACK>


  
  if (whoami == 0 &&
	  g_conf.num_osd > 4 &&
	  g_conf.fake_osdmap_expand) {
	dout(1) << "scheduling OSD map reorg at " << g_conf.fake_osdmap_expand << endl;
	g_timer.add_event_after(g_conf.fake_osdmap_expand,
							new C_OM_Faker(this));
  }

  if (whoami == 0) {
	// fake osd failures
	for (map<int,float>::iterator i = g_fake_osd_down.begin();
		 i != g_fake_osd_down.end();
		 i++) {
	  dout(0) << "will fake osd" << i->first << " DOWN after " << i->second << endl;
	  g_timer.add_event_after(i->second, new C_OM_FakeOSDFailure(this, i->first, 1));
	}
	for (map<int,float>::iterator i = g_fake_osd_out.begin();
		 i != g_fake_osd_out.end();
		 i++) {
	  dout(0) << "will fake osd" << i->first << " OUT after " << i->second << endl;
	  g_timer.add_event_after(i->second, new C_OM_FakeOSDFailure(this, i->first, 0));
	}
  }

  
  // i'm ready!
  messenger->set_dispatcher(this);
  
  // start ticker
  g_timer.add_event_after(g_conf.mon_tick_interval, new C_OM_PingTick(messenger));
}


void OSDMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_FAILURE:
	handle_failure((MFailure*)m);
	break;
	
  case MSG_PING_ACK:
	handle_ping_ack((MPingAck*)m);
	break;

  case MSG_OSD_GETMAP:
	handle_osd_getmap((MOSDGetMap*)m);
	return;

  case MSG_OSD_BOOT:
	handle_osd_boot((MOSDBoot*)m);
	return;

  case MSG_SHUTDOWN:
	handle_shutdown(m);
	return;

  case MSG_PING:
	tick();
	delete m;
	return;

  default:
	dout(0) << "unknown message " << *m << endl;
	assert(0);
  }
}


void OSDMonitor::handle_shutdown(Message *m)
{
  dout(1) << "shutdown from " << m->get_source() << endl;
  messenger->shutdown();
  delete messenger;
  delete m;
}

void OSDMonitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}

void OSDMonitor::handle_failure(MFailure *m)
{
  dout(1) << "osd failure: " << m->get_failed() << " from " << m->get_source() << endl;
  
  // ack
  messenger->send_message(new MFailureAck(m),
						  m->get_source(), m->get_source_port());

  // FIXME
  // take their word for it
  int from = m->get_failed().num();
  if (osdmap->is_up(from) &&
	  (osdmap->osd_inst.count(from) == 0 ||
	   osdmap->osd_inst[from] == m->get_inst())) {
	pending.new_down[from] = m->get_inst();
	accept_pending();

	if (osdmap->is_in(from))
	  pending_out[from] = g_clock.now();
	
	bcast_latest_osd_map_mds();   
	bcast_latest_osd_map_osd();   // FIXME: which osds can i tell?

	//send_incremental_map(osdmap->get_epoch()-1, m->get_source(), true);
  }

  delete m;
}



void OSDMonitor::fake_osd_failure(int osd, bool down) 
{
  if (down) {
	dout(1) << "fake_osd_failure DOWN osd" << osd << endl;
	pending.new_down[osd] = osdmap->osd_inst[osd];
  } else {
	dout(1) << "fake_osd_failure OUT osd" << osd << endl;
	pending.new_out.push_back(osd);
  }
  accept_pending();
  bcast_latest_osd_map_osd();
  bcast_latest_osd_map_mds();
}


void OSDMonitor::handle_osd_boot(MOSDBoot *m)
{
  dout(-7) << "osd_boot from " << m->get_source() << endl;
  assert(m->get_source().is_osd());
  int from = m->get_source().num();

  if (osdmap->get_epoch() == 1) {
	// initial boot.  hackish.  FIXME.
	osdmap->osd_inst[from] = m->get_source_inst();
  } else {
 
	// already up?  mark down first?
	if (osdmap->is_up(from)) {
	  assert(m->get_source_inst() > osdmap->osd_inst[from]);   // this better be newer!  
	  pending.new_down[from] = osdmap->osd_inst[from];
	  accept_pending();
	}

	// mark up.
	pending_out.erase(from);
	assert(osdmap->is_down(from));
	pending.new_up[from] = m->get_source_inst();

	// mark in?
	if (osdmap->out_osds.count(from)) 
	  pending.new_in.push_back(from);

	accept_pending();
  }

  // the booting osd will spread word
  send_incremental_map(m->sb.current_epoch, m->get_source(), true);
  delete m;

  // tell mds
  bcast_latest_osd_map_mds();
}

void OSDMonitor::handle_osd_getmap(MOSDGetMap *m)
{
  dout(7) << "osd_getmap from " << m->get_source() << " since " << m->get_since() << endl;
  
  if (m->get_since())
	send_incremental_map(m->get_since(), m->get_source(), true);
  else
	send_full_map(m->get_source());
  delete m;
}



void OSDMonitor::accept_pending()
{
  dout(-10) << "accept_pending " << osdmap->get_epoch() << " -> " << pending.epoch << endl;

  // accept pending into a new map!
  pending.encode( inc_maps[ pending.epoch ] );
  
  // advance!
  osdmap->apply_incremental(pending);

  
  // tell me about it
  for (map<int,entity_inst_t>::iterator i = pending.new_up.begin();
	   i != pending.new_up.end(); 
	   i++) { 
	dout(0) << "osd" << i->first << " UP " << i->second << endl;
	derr(0) << "osd" << i->first << " UP " << i->second << endl;
	messenger->mark_up(MSG_ADDR_OSD(i->first), i->second);
  }
  for (map<int,entity_inst_t>::iterator i = pending.new_down.begin();
	   i != pending.new_down.end();
	   i++) {
	dout(0) << "osd" << i->first << " DOWN " << i->second << endl;
	derr(0) << "osd" << i->first << " DOWN " << i->second << endl;
	messenger->mark_down(MSG_ADDR_OSD(i->first), i->second);
  }
  for (list<int>::iterator i = pending.new_in.begin();
	   i != pending.new_in.end();
	   i++) {
	dout(0) << "osd" << *i << " IN" << endl;
	derr(0) << "osd" << *i << " IN" << endl;
  }
  for (list<int>::iterator i = pending.new_out.begin();
	   i != pending.new_out.end();
	   i++) {
	dout(0) << "osd" << *i << " OUT" << endl;
	derr(0) << "osd" << *i << " OUT" << endl;
  }

  // clear new pending
  OSDMap::Incremental next(osdmap->get_epoch() + 1);
  pending = next;
}


void OSDMonitor::send_full_map(msg_addr_t who)
{
  messenger->send_message(new MOSDMap(osdmap), who);
}

void OSDMonitor::send_incremental_map(epoch_t since, msg_addr_t dest, bool full)
{
  dout(-10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
		   << " to " << dest << endl;
  
  MOSDMap *m = new MOSDMap;
  
  for (epoch_t e = osdmap->get_epoch();
	   e > since;
	   e--) {
	bufferlist bl;
	if (inc_maps.count(e)) {
	  dout(-10) << "send_incremental_map    inc " << e << endl;
	  m->incremental_maps[e] = inc_maps[e];
	} else if (maps.count(e)) {
	  dout(-10) << "send_incremental_map   full " << e << endl;
	  m->maps[e] = maps[e];
	  if (!full) break;
	}
	else {
	  assert(0);  // we should have all maps.
	}
  }
  
  messenger->send_message(m, dest);
}



void OSDMonitor::bcast_latest_osd_map_mds()
{
  epoch_t e = osdmap->get_epoch();
  dout(1) << "bcast_latest_osd_map_mds epoch " << e << endl;
  
  // tell mds
  for (int i=0; i<g_conf.num_mds; i++) {
	//send_full_map(MSG_ADDR_MDS(i));
	send_incremental_map(osdmap->get_epoch()-1, MSG_ADDR_MDS(i), false);
  }
}

void OSDMonitor::bcast_latest_osd_map_osd()
{
  epoch_t e = osdmap->get_epoch();
  dout(1) << "bcast_latest_osd_map_osd epoch " << e << endl;

  // tell osds
  set<int> osds;
  osdmap->get_all_osds(osds);
  for (set<int>::iterator it = osds.begin();
	   it != osds.end();
	   it++) {
	if (osdmap->is_down(*it)) continue;

	send_incremental_map(osdmap->get_epoch()-1, MSG_ADDR_OSD(*it), false);
  }  
}



void OSDMonitor::tick()
{
  dout(10) << "tick" << endl;

  // mark down osds out?
  utime_t now = g_clock.now();
  list<int> mark_out;
  for (map<int,utime_t>::iterator i = pending_out.begin();
	   i != pending_out.end();
	   i++) {
	utime_t down = now;
	down -= i->second;

	if (down.sec() >= g_conf.mon_osd_down_out_interval) {
	  dout(10) << "tick marking osd" << i->first << " OUT after " << down << " sec" << endl;
	  mark_out.push_back(i->first);
	}
  }
  for (list<int>::iterator i = mark_out.begin();
	   i != mark_out.end();
	   i++) {
	pending_out.erase(*i);
	pending.new_out.push_back( *i );
	accept_pending();
  }
  
  // next!
  g_timer.add_event_after(g_conf.mon_tick_interval, new C_OM_PingTick(messenger));
}
