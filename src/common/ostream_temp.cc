// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

OstreamTemp::OstreamTemp(clog_type type_, LogChannel &parent_)
  : type(type_), parent(parent_)
{
}

OstreamTemp::~OstreamTemp()
{
  if (ss.peek() != EOF)
    parent.do_log(type, ss);
}

