// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"

class MOSDOp;

namespace ceph::osd {
class OSD;

class ClientRequest final : public OperationT<ClientRequest> {
  OSD &osd;
  Ref<MOSDOp> m;
  
public:
  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, Ref<MOSDOp> &&m);

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
  seastar::future<> start();
};

}
