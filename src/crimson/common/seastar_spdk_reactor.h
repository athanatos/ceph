// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

seastar::future<> spdk_reactor_start();
seastar::future<> spdk_reactor_stop();
