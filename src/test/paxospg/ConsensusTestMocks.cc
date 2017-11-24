// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ConsensusTestMocks.h"

Ceph::Future<Count> Counter::process_command(
	const Increment &c,
	ObjectStore::Transaction *t)
{
	// TODO
	return Ceph::Future<Count>::make_ready_value(1);
}
