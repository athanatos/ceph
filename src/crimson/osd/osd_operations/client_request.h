// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd_operation.h"

namespace ceph::osd {

class ClientRequest final : public OperationT<ClientRequest> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
  seastar::future<> start();
};

}
